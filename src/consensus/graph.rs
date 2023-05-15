use std::collections::VecDeque;
use std::task::Poll;
use std::{fmt::Debug, sync::Arc};

use futures::Stream;
use futures::{Future, StreamExt};
use libp2p::PeerId;
use pin_project_lite::pin_project;
use rust_hashgraph::algorithm::datastructure::{self, EventCreateError, Graph};
use rust_hashgraph::algorithm::event::Hash;
use rust_hashgraph::algorithm::PushError;
use rust_hashgraph::algorithm::{Clock, Signer};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::pin;
use tokio::sync::Notify;
use tracing::{error, info, trace, warn};

use crate::module::ModuleChannelServer;
use crate::signatures::EncodedEd25519Pubkey;
use crate::types::{GraphSync, Sid, Vid};

use super::Transaction;

pub struct Module;

impl crate::module::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type SharedState = ();
}

pub enum OutEvent {
    FinalizedTransaction {
        from: PeerId,
        event_hash: Hash,
        tx: Transaction<Vid, Sid, PeerId>,
    },
    SyncReady {
        to: PeerId,
        sync: GraphSync,
    },
}

pub enum InEvent {
    ApplySync { from: PeerId, sync: GraphSync },
    GenerateSync { to: PeerId },
    ScheduleTx(Transaction<Vid, Sid, PeerId>),
    CreateStandalone,
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
    pub struct GraphWrapper<TDataId, TShardId, TSigner, TClock> {
        // todo: replace parentheses - ()
        inner: Graph<EventPayload<TDataId, TShardId>, GenesisPayload, PeerId, TSigner, TClock>,
        state_updated: Arc<Notify>,
        included_transaction_buffer: Vec<Transaction<TDataId, TShardId, PeerId>>,
        retrieved_transaction_buffer: (PeerId, VecDeque<Transaction<TDataId, TShardId, PeerId>>, Hash),
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
            retrieved_transaction_buffer: (
                PeerId::random(),
                VecDeque::new(),
                Hash::from_array([0; 64]),
            ),
        }
    }

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

impl<TDataId, TShardId, TSigner, TClock> Stream for GraphWrapper<TDataId, TShardId, TSigner, TClock>
where
    TDataId: Serialize + Eq + std::hash::Hash + Debug + Clone,
    TShardId: Serialize + Eq + std::hash::Hash + Debug + Clone,
    TSigner: Signer<GenesisPayload, SignerIdentity = PeerId>,
    TClock: Clock,
{
    type Item = (PeerId, Transaction<TDataId, TShardId, PeerId>, Hash);

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // wake up if something changed
        let state_updated_notification = this.state_updated.notified();
        pin!(state_updated_notification);
        let _ = state_updated_notification.poll(cx);

        loop {
            if let Some(tx) = this.retrieved_transaction_buffer.1.pop_front() {
                // feed transactions from an event one by one
                return Poll::Ready(Some((
                    this.retrieved_transaction_buffer.0,
                    tx,
                    this.retrieved_transaction_buffer.2.clone(),
                )));
            } else {
                // no txs left in previous event, getting a new one
                match this.inner.next_event() {
                    Some(event) => {
                        let author = event.author().clone();
                        let mut txs: VecDeque<_> = event.payload().transactions.clone().into();
                        let next_tx = txs.pop_front();
                        this.retrieved_transaction_buffer.0 = author;
                        this.retrieved_transaction_buffer.1 = txs;
                        match next_tx {
                            Some(tx) => {
                                return Poll::Ready(Some((author, tx, event.hash().clone())))
                            }
                            None => {
                                // more events might be available
                                continue;
                            }
                        }
                    }
                    None => return Poll::Pending,
                }
            }
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
                    let Some((from, tx, event_hash)) = next_tx else {
                        info!("stream of events ended, shuttung down consensus");
                        return;
                    };
                    if let Err(_) = connection.output.send(OutEvent::FinalizedTransaction { from, tx, event_hash }).await {
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
                        InEvent::ApplySync { from, sync } => {
                            if let Err(e) = self.apply_sync(from, sync) {
                                warn!("Failed to apply sync from peer {}: {}", from, e);
                            }
                        },
                        InEvent::GenerateSync { to } => {
                            let sync = match self.inner.generate_sync_for(&to) {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("Graph state inconsistent or bug in generation of sync: {:?}", e);
                                    // todo: maybe store state to debug???
                                    return;
                                },
                            };
                            // todo: maybe use `try_send` or `reserve` on each send
                            if let Err(_) = connection.output.send(OutEvent::SyncReady { to, sync }).await {
                                error!("`connection.output` is closed, shuttung down consensus");
                                return;
                            }
                        },
                        InEvent::ScheduleTx(tx) => {
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
