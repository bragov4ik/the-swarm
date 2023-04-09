use std::fmt::Debug;

use futures::Stream;
use libp2p::{swarm::NetworkBehaviour, PeerId};
use rust_hashgraph::algorithm::{
    datastructure::{self, EventCreateError, Graph},
    PushError,
};
use serde::Serialize;
use thiserror::Error;
use tracing::debug;

use super::Transaction;

pub struct GraphWrapper<TDataId, TPieceId> {
    // todo: replace parentheses - ()
    inner: Graph<EventPayload<TDataId, TPieceId>, PeerId, (), ()>,
    transaction_buffer: Vec<Transaction<TDataId, TPieceId, PeerId>>,
}

#[derive(Serialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
struct EventPayload<TDataId, TPieceId> {
    transacitons: Vec<Transaction<TDataId, TPieceId, PeerId>>,
}

// enum GraphRequest<TDataId, TPieceId> {
//     PerformSync(datastructure::sync::Jobs<EventPayload<TDataId, TPieceId>, PeerId>)
// }

impl<TDataId, TPieceId> GraphWrapper<TDataId, TPieceId> {
    pub fn inner(&self) -> &Graph<EventPayload<TDataId, TPieceId>, PeerId, (), ()> {
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

impl<TDataId, TPieceId> GraphWrapper<TDataId, TPieceId>
where
    TDataId: Serialize + Eq + std::hash::Hash + Debug + Clone,
    TPieceId: Serialize + Eq + std::hash::Hash + Debug + Clone,
{
    pub fn apply_sync(
        &mut self,
        from: &PeerId,
        sync_jobs: datastructure::sync::Jobs<EventPayload<TDataId, TPieceId>, PeerId>,
    ) -> Result<(), ApplySyncError> {
        let other_parent = self
            .inner
            .peer_latest_event(from)
            .clone()
            .ok_or_else(|| ApplySyncError::UnknownPeer(from.clone()))?;
        for next_event in sync_jobs.into_linear() {
            self.inner.push_event(next_event)?;
        }
        let payload = EventPayload {
            transacitons: self.transaction_buffer,
        };
        self.inner.create_event(payload, other_parent.clone())?;
        Ok(())
    }
}

impl<TDataId, TPieceId> Stream for GraphWrapper<TDataId, TPieceId> {
    type Item = Transaction<TDataId, TPieceId, PeerId>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        tokio::select! {}
    }
}
