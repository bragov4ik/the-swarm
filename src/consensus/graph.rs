use std::collections::VecDeque;
use std::task::Poll;
use std::{fmt::Debug, sync::Arc};

use futures::Stream;
use futures::{Future, StreamExt};
use libp2p::PeerId;
use pin_project_lite::pin_project;
use rust_hashgraph::algorithm::datastructure::{self, EventCreateError, Graph};
use rust_hashgraph::algorithm::event::{EventWrapper, Hash};
use rust_hashgraph::algorithm::PushError;
use rust_hashgraph::algorithm::{Clock, Signer};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::pin;
use tokio::sync::Notify;
use tracing::{debug, error, info, trace, warn};

use crate::logging_helpers::Targets;
use crate::module::ModuleChannelServer;
use crate::signatures::EncodedEd25519Pubkey;
use crate::types::{GraphSync, Sid, Vid};

use super::Transaction;

pub struct Module;

impl crate::module::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type SharedState = ModuleState;
}

#[derive(Debug, Clone)]
pub enum OutEvent {
    GenerateSyncResponse {
        to: PeerId,
        sync: GraphSync,
    },
    KnownPeersResponse(Vec<PeerId>),
    /// This transaction is confirmed to be seen by supermajority
    /// of the peers and its ordering is univocally decided by
    /// consensus.
    FinalizedTransaction {
        from: PeerId,
        event_hash: Hash,
        tx: Transaction<Vid, Sid, PeerId>,
    },
    /// This transaction (tx) is not guaranteed to be seen by supermajority
    /// of the peers; the ordering (of events) might change in finalized
    /// version.
    ///
    /// Note that the order of recognized events (and, thus,
    /// txs) follows ancestry relationship. I.e. if event $A$ is an ancestor
    /// of event $B$, `RecognizedTransaction`'s for $A$'s txs will be emitted
    /// before `RecognizedTransaction`'s for $B$
    ///
    /// ## Applications
    ///
    /// They still can be useful for example for:
    /// 1. Speculative execution; a peer can start to perform some actions
    /// associated with the transaction and have the result ready when
    /// the transaction actually finalizes.
    /// 2. Relying in special cases of the network, such as single peer
    /// making all "command" transactions (txs switching order of which
    /// can actually make a difference). Due to ancestry between all its events,
    /// all "command" txs are going to be ordered properly, making it possible to
    /// react to them right away and to improve system usability & responsiveness
    RecognizedTransaction {
        from: PeerId,
        event_hash: Hash,
        tx: Transaction<Vid, Sid, PeerId>,
    },
}

#[derive(Debug, Clone)]
pub enum InEvent {
    GenerateSyncRequest { to: PeerId },
    // get list of known peers to the consensus
    KnownPeersRequest,
    ApplySync { from: PeerId, sync: GraphSync },
    ScheduleTx(Transaction<Vid, Sid, PeerId>),
    CreateStandalone,
}

pub enum ModuleState {
    Ready,
    Busy,
}

impl crate::module::State for ModuleState {
    fn accepts_input(&self) -> bool {
        match self {
            ModuleState::Ready => true,
            ModuleState::Busy => false,
        }
    }
}

pub type SyncJobs<TDataId, TShardId> =
    datastructure::sync::Jobs<EventPayload<TDataId, TShardId>, GenesisPayload, PeerId>;

#[derive(Serialize, Deserialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
pub struct EventPayload<TDataId, TShardId> {
    transactions: Vec<Transaction<TDataId, TShardId, PeerId>>,
}

impl<TDataId, TShardId> EventPayload<TDataId, TShardId> {
    pub fn new(transactions: Vec<Transaction<TDataId, TShardId, PeerId>>) -> Self {
        Self { transactions }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
pub struct GenesisPayload {
    pub pubkey: EncodedEd25519Pubkey,
}

pin_project! {
    /// Async wrapper for the graph consensus. Intended to communicate
    /// with behaviour through corresponding [`ModuleChannelServer`] (the
    /// behaviour thus uses [`ModuleChannelClient`]).
    ///
    /// Use [`Self::from_graph()`] to create, [`Self::run()`] to operate.
    #[project = GraphWrapperProjection]
    pub struct GraphWrapper<TDataId, TShardId, TSigner, TClock> {
        inner: Graph<EventPayload<TDataId, TShardId>, GenesisPayload, PeerId, TSigner, TClock>,
        // Notification for possible new finalized/recognized transactions
        state_updated: Arc<Notify>,
        // transactions scheduled for inclusion into a next event
        included_transaction_buffer: Vec<Transaction<TDataId, TShardId, PeerId>>,
        // transactions from latest finalized transaction
        finalized_transaction_buffer: (PeerId, VecDeque<Transaction<TDataId, TShardId, PeerId>>, Hash),
        // transactions from latest recognized (seen in partial order) event
        recognized_transaction_buffer: (PeerId, VecDeque<Transaction<TDataId, TShardId, PeerId>>, Hash),
    }
}
impl<TDataId, TShardId, TSigner, TClock> GraphWrapper<TDataId, TShardId, TSigner, TClock> {
    pub fn from_graph(
        graph: Graph<EventPayload<TDataId, TShardId>, GenesisPayload, PeerId, TSigner, TClock>,
    ) -> Self {
        Self {
            inner: graph,
            state_updated: Arc::new(Notify::new()),
            included_transaction_buffer: Vec::new(),
            finalized_transaction_buffer: (
                PeerId::random(),
                VecDeque::new(),
                Hash::from_array([0; 64]),
            ),
            recognized_transaction_buffer: (
                PeerId::random(),
                VecDeque::new(),
                Hash::from_array([0; 64]),
            ),
        }
    }

    // might be useful
    #[allow(unused)]
    pub fn inner(
        &self,
    ) -> &Graph<EventPayload<TDataId, TShardId>, GenesisPayload, PeerId, TSigner, TClock> {
        &self.inner
    }
}

#[derive(Error, Debug)]
pub enum ApplySyncError {
    #[error(transparent)]
    PushError(#[from] PushError<PeerId>),
    #[error("Peer `from` is unknown")]
    UnknownPeer(PeerId),
    #[error("Failed to create new gossip event")]
    CreateError(#[from] EventCreateError<PeerId>),
}

impl<TDataId, TShardId, TSigner, TClock> GraphWrapper<TDataId, TShardId, TSigner, TClock>
where
    TDataId: Serialize + Eq + std::hash::Hash + Debug + Clone,
    TShardId: Serialize + Eq + std::hash::Hash + Debug + Clone,
    TSigner: Signer<GenesisPayload, SignerIdentity = PeerId>,
    TClock: Clock,
{
    pub fn apply_sync(
        &mut self,
        from: PeerId,
        sync_jobs: SyncJobs<TDataId, TShardId>,
    ) -> Result<(), ApplySyncError> {
        debug!("Applying sync with {} jobs", sync_jobs.as_linear().len());
        if !sync_jobs.as_linear().is_empty() {
            self.state_updated.notify_one();
        }
        for next_event in sync_jobs.into_linear() {
            let (next_event, signature) = next_event.into_parts();
            match self.inner.push_event(next_event, signature) {
                Ok(()) => (),
                Err(PushError::EventAlreadyExists(hash)) => {
                    trace!("Received event {} is already known, skipping", hash)
                }
                Err(e) => return Err(e.into()),
            };
        }
        let txs = std::mem::take(&mut self.included_transaction_buffer);
        let payload = EventPayload { transactions: txs };
        // Retrieving the parent after applying sync, because the latest event is likely
        // to be updated there.
        let other_parent = self
            .inner
            .peer_latest_event(&from)
            .clone()
            .ok_or_else(|| ApplySyncError::UnknownPeer(from.clone()))?;
        self.inner.create_event(payload, other_parent.clone())?;
        self.included_transaction_buffer.clear();
        Ok(())
    }

    pub fn create_standalone_event(&mut self) -> Result<(), EventCreateError<PeerId>> {
        self.state_updated.notify_one();
        let txs = std::mem::take(&mut self.included_transaction_buffer);
        let payload = EventPayload { transactions: txs };
        let self_parent = self
            .inner
            .peer_latest_event(self.inner.self_id())
            .expect("Peer must know itself")
            .clone();
        self.inner.create_event(payload, self_parent)?;
        Ok(())
    }

    pub fn push_tx(&mut self, tx: Transaction<TDataId, TShardId, PeerId>) {
        self.included_transaction_buffer.push(tx);
    }
}

pub type NextTxData<TDataId, TShardId> = (PeerId, Transaction<TDataId, TShardId, PeerId>, Hash);

pub enum NextTx<TDataId, TShardId> {
    Recognized(NextTxData<TDataId, TShardId>),
    Finalized(NextTxData<TDataId, TShardId>),
}

impl<TDataId, TShardId, TSigner, TClock> GraphWrapper<TDataId, TShardId, TSigner, TClock>
where
    TDataId: Serialize + Eq + std::hash::Hash + Debug + Clone + 'static,
    TShardId: Serialize + Eq + std::hash::Hash + Debug + Clone + 'static,
    TSigner: Signer<GenesisPayload, SignerIdentity = PeerId>,
    TClock: Clock,
{
    // todo: replace `graph` with function
    fn next_tx<'a, F>(
        this: &mut Graph<EventPayload<TDataId, TShardId>, GenesisPayload, PeerId, TSigner, TClock>,
        get_next_event: F,
        tx_buffer: &mut (
            PeerId,
            VecDeque<Transaction<TDataId, TShardId, PeerId>>,
            Hash,
        ),
    ) -> Option<NextTxData<TDataId, TShardId>>
    where
        F: for<'b> Fn(
            &'b mut Graph<EventPayload<TDataId, TShardId>, GenesisPayload, PeerId, TSigner, TClock>,
        ) -> Option<
            &'b EventWrapper<EventPayload<TDataId, TShardId>, GenesisPayload, PeerId>,
        >,
    {
        loop {
            if let Some(tx) = tx_buffer.1.pop_front() {
                // feed transactions from an event one by one
                return Some((tx_buffer.0, tx, tx_buffer.2.clone()));
            } else {
                // no txs left in previous event, getting a new one
                match get_next_event(this) {
                    Some(event) => {
                        let author = event.author().clone();
                        let mut txs: VecDeque<_> = event.payload().transactions.clone().into();
                        let next_tx = txs.pop_front();
                        tx_buffer.0 = author;
                        tx_buffer.1 = txs;
                        match next_tx {
                            Some(tx) => return Some((author, tx, event.hash().clone())),
                            None => {
                                // more events might be available
                                continue;
                            }
                        }
                    }
                    None => return None,
                }
            }
        }
    }

    // If `None` then it might not be available yet
    fn next_finalized(
        this: &mut GraphWrapperProjection<'_, TDataId, TShardId, TSigner, TClock>,
    ) -> Option<NextTx<TDataId, TShardId>> {
        Self::next_tx(
            this.inner,
            |inner| inner.next_finalized_event(),
            this.finalized_transaction_buffer,
        )
        .map(|d| NextTx::Finalized(d))
    }

    // If `None` then it might not be available yet
    fn next_recognized(
        this: &mut GraphWrapperProjection<'_, TDataId, TShardId, TSigner, TClock>,
    ) -> Option<NextTx<TDataId, TShardId>> {
        Self::next_tx(
            this.inner,
            |inner| inner.next_recognized_event(),
            this.finalized_transaction_buffer,
        )
        .map(|d| NextTx::Recognized(d))
    }
}

impl<TDataId, TShardId, TSigner, TClock> Stream for GraphWrapper<TDataId, TShardId, TSigner, TClock>
where
    TDataId: Serialize + Eq + std::hash::Hash + Debug + Clone + 'static,
    TShardId: Serialize + Eq + std::hash::Hash + Debug + Clone + 'static,
    TSigner: Signer<GenesisPayload, SignerIdentity = PeerId>,
    TClock: Clock,
{
    type Item = NextTx<TDataId, TShardId>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // wake up if something changed
        {
            let state_updated_notification = this.state_updated.notified();
            pin!(state_updated_notification);
            let _ = state_updated_notification.poll(cx);
        }
        match Self::next_finalized(&mut this).or_else(|| Self::next_recognized(&mut this)) {
            // we're assuming infinite stream so no `None` is returned
            Some(tx) => Poll::Ready(Some(tx)),
            None => Poll::Pending,
        }
    }
}

impl<TSigner, TClock> GraphWrapper<Vid, Sid, TSigner, TClock>
where
    TSigner: Signer<GenesisPayload, SignerIdentity = PeerId>,
    TClock: Clock,
{
    pub async fn run(mut self, mut connection: ModuleChannelServer<Module>) {
        loop {
            tokio::select! {
                next_tx = self.next() => {
                    let Some(next_tx) = next_tx else {
                        info!("stream of events ended, shuttung down consensus");
                        return;
                    };
                    let out_event = match next_tx {
                        NextTx::Recognized((from, tx, event_hash)) => OutEvent::RecognizedTransaction { from, tx, event_hash },
                        NextTx::Finalized((from, tx, event_hash)) => OutEvent::FinalizedTransaction { from, tx, event_hash },
                    };
                    if let Err(_) = connection.output.send(out_event).await {
                        info!("`connection.output` is closed, shuttung down consensus");
                        return;
                    }
                }
                in_event = connection.input.recv() => {
                    let Some(in_event) = in_event else {
                        info!("`connection.output` is closed, shuttung down consensus");
                        return;
                    };
                    match in_event {
                        InEvent::GenerateSyncRequest { to } => {
                            debug!("Generating sync for {:?}", to);
                            connection.set_state(ModuleState::Busy);
                            let sync = match self.inner.generate_sync_for(&to) {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("Graph state inconsistent or bug in generation of sync: {:?}", e);
                                    // todo: maybe store state to debug???
                                    return;
                                },
                            };
                            connection.set_state(ModuleState::Ready);
                            // todo: maybe use `try_send` or `reserve` on each send
                            if let Err(_) = connection.output.send(OutEvent::GenerateSyncResponse { to, sync }).await {
                                error!("`connection.output` is closed, shuttung down consensus");
                                return;
                            }
                        },
                        InEvent::KnownPeersRequest => {
                            if let Err(_) = connection.output.send(OutEvent::KnownPeersResponse(self.inner.peers())).await {
                                error!("`connection.output` is closed, shuttung down consensus");
                                return;
                            }
                        }
                        InEvent::ApplySync { from, sync } => {
                            connection.set_state(ModuleState::Busy);
                            let apply_result = self.apply_sync(from, sync);
                            connection.set_state(ModuleState::Ready);
                            if let Err(e) = apply_result {
                                warn!("Failed to apply sync from peer {}: {}", from, e);
                            }
                        },
                        InEvent::ScheduleTx(tx) => {
                            match &tx {
                                Transaction::InitializeStorage { distribution: _ } =>
                                debug!(target: Targets::StorageInitialization.into_str(), "Scheduling init transaction for inclusion in event"),
                                _ => (),
                            }
                            self.push_tx(tx);
                        },
                        InEvent::CreateStandalone => {
                            if let Err(e) = self.create_standalone_event() {
                                warn!("Failed to create standalone event: {}", e);
                            }
                        },
                    }
                }
                _ = connection.shutdown.cancelled() => {
                    info!("received cancel signal, shutting down consensus");
                    return;
                }
            }
        }
    }
}
