use std::collections::VecDeque;
use std::task::Poll;
use std::{fmt::Debug, sync::Arc};

use futures::Future;
use futures::Stream;
use libp2p::PeerId;
use pin_project_lite::pin_project;
use rust_hashgraph::algorithm::{
    datastructure::{self, EventCreateError, Graph},
    PushError,
};
use rust_hashgraph::algorithm::{Clock, Signer};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::pin;
use tokio::sync::Notify;

use crate::types::{GraphSync, Sid, Vid};

use super::{GraphConsensus, Transaction};

pub struct Module;

impl crate::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type State = ();
}

pub enum OutEvent {
    FinalizedTransaction {
        from: PeerId,
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
    datastructure::sync::Jobs<EventPayload<TDataId, TShardId>, PeerId>;

pin_project! {
    pub struct GraphWrapper<TDataId, TPieceId, TSigner, TClock> {
        // todo: replace parentheses - ()
        inner: Graph<EventPayload<TDataId, TPieceId>, PeerId, TSigner, TClock>,
        state_updated: Arc<Notify>,
        included_transaction_buffer: Vec<Transaction<TDataId, TPieceId, PeerId>>,
        retrieved_transaction_buffer: (PeerId, VecDeque<Transaction<TDataId, TPieceId, PeerId>>),
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
pub struct EventPayload<TDataId, TPieceId> {
    transactions: Vec<Transaction<TDataId, TPieceId, PeerId>>,
}

impl<TDataId, TPieceId, TSigner, TClock> GraphWrapper<TDataId, TPieceId, TSigner, TClock> {
    pub fn from_graph(
        graph: Graph<EventPayload<TDataId, TPieceId>, PeerId, TSigner, TClock>,
    ) -> Self {
        Self {
            inner: graph,
            state_updated: Arc::new(Notify::new()),
            included_transaction_buffer: Vec::new(),
            retrieved_transaction_buffer: (PeerId::random(), VecDeque::new()),
        }
    }

    pub fn inner(&self) -> &Graph<EventPayload<TDataId, TPieceId>, PeerId, TSigner, TClock> {
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

impl<TDataId, TPieceId, TSigner, TClock> GraphWrapper<TDataId, TPieceId, TSigner, TClock>
where
    TDataId: Serialize + Eq + std::hash::Hash + Debug + Clone,
    TPieceId: Serialize + Eq + std::hash::Hash + Debug + Clone,
    TSigner: Signer<SignerIdentity = PeerId>,
    TClock: Clock,
{
    pub fn apply_sync(
        &mut self,
        from: PeerId,
        sync_jobs: SyncJobs<TDataId, TPieceId>,
    ) -> Result<(), ApplySyncError> {
        if !sync_jobs.as_linear().is_empty() {
            self.state_updated.notify_one();
        }
        for next_event in sync_jobs.into_linear() {
            let (next_event, signature) = next_event.into_parts();
            self.inner.push_event(next_event, signature)?;
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
}

impl<TDataId, TPieceId, TSigner, TClock> GraphConsensus
    for GraphWrapper<TDataId, TPieceId, TSigner, TClock>
where
    TDataId: Serialize + Eq + std::hash::Hash + Debug + Clone,
    TPieceId: Serialize + Eq + std::hash::Hash + Debug + Clone,
    TSigner: Signer<SignerIdentity = PeerId>,
    TClock: Clock,
{
    type OperandId = TDataId;
    type OperandPieceId = TPieceId;
    type PeerId = PeerId;
    type SyncPayload = (PeerId, SyncJobs<TDataId, TPieceId>);
    type UpdateError = ApplySyncError;
    type PushTxError = ();
    type SyncGenerateError = datastructure::sync::Error;

    fn update_graph(&mut self, update: Self::SyncPayload) -> Result<(), Self::UpdateError> {
        self.apply_sync(update.0, update.1)
    }

    fn get_sync(
        &self,
        sync_for: &Self::PeerId,
    ) -> Result<Self::SyncPayload, Self::SyncGenerateError> {
        let sync_payload = self.inner.generate_sync_for(sync_for)?;
        Ok((self.inner.self_id().clone(), sync_payload))
    }

    fn push_tx(
        &mut self,
        tx: Transaction<Self::OperandId, Self::OperandPieceId, Self::PeerId>,
    ) -> Result<(), Self::PushTxError> {
        self.included_transaction_buffer.push(tx);
        Ok(())
    }
}

impl<TDataId, TPieceId, TSigner, TClock> Stream for GraphWrapper<TDataId, TPieceId, TSigner, TClock>
where
    TDataId: Serialize + Eq + std::hash::Hash + Debug + Clone,
    TPieceId: Serialize + Eq + std::hash::Hash + Debug + Clone,
    TSigner: Signer<SignerIdentity = PeerId>,
    TClock: Clock,
{
    type Item = (PeerId, Transaction<TDataId, TPieceId, PeerId>);

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let state_updated_notification = this.state_updated.notified();
        pin!(state_updated_notification);
        state_updated_notification.poll(cx);
        loop {
            if let Some(tx) = this.retrieved_transaction_buffer.1.pop_front() {
                // feed transactions from an event one by one
                return Poll::Ready(Some((this.retrieved_transaction_buffer.0, tx)));
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
                            Some(tx) => return Poll::Ready(Some((author, tx))),
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
