//! Consensus and its main functions.
//!
//! ## Description
//!
//! The intention is to utulize the graph consensus in 3 ways:
//! - Reaching agreement on system state
//! - Instruction memory (since it seems that storing requested
//! instructions as transactions is a good idea)
//! - Data discovery (we can also store the information about a node holding shard of a
//! certain data unit as transaction)
//!
//! For this we have corresponding traits [GraphConsensus], [DataDiscoverer]
//! ([InstructionMemory] is in a separate module, because it makes sense to have it
//! together with data memory).
//!
//! ## Current implementation
//!
//! Due to the lack of time, now there is only a mock consensus that does not provide
//! Byzantine fault tolerance and proper security.
//!
//! In the future the same traits are expected to be implemented for the actual consensus
//! protocol to achieve a working distributed system. The implementations shouldn't be too
//! complicated as long as the consensus itself is ready, since the required funcitons are
//! easily done when the protocol is ready:
//! - Updating graph is a part of protocol
//! - Adding transactions easily done via tx buffer + creating events
//! - Getting next tx is easy when we have an linear ordering of events
//! - Shard locations can be obtained by searching txs with announcements of holding a
//! vector shard in the graph (can be cached as well to reduce complexity)
//! - Adding/getting instructions is basically adding/getting transaction of type
//! instruction"

use serde::{Deserialize, Serialize};

use crate::processor::Instruction;

pub mod mock;
// mod graph;

pub trait GraphConsensus {
    type OperandId;
    type OperandPieceId;

    /// Peer identifier
    type PeerId;

    /// Data that is transferred for peers sync.
    /// Something like list of events that source peer knows.
    type SyncPayload;

    type UpdateError;
    type PushTxError;

    /// Update local knowledge of the graph according to received gossip
    fn update_graph(&mut self, new_graph: Self::SyncPayload) -> Result<(), Self::UpdateError>;

    /// Get graph state to send to peers
    fn get_graph(&self) -> Self::SyncPayload;

    /// Add transaction to a queue - list of txs that will be added in next event
    /// created by this node.
    fn push_tx(
        &mut self,
        tx: Transaction<Self::OperandId, Self::OperandPieceId, Self::PeerId>,
    ) -> Result<(), Self::PushTxError>;
}

pub trait DataDiscoverer {
    /// ID used to distinguish data
    type DataIdentifier;

    /// Address of location data is located at
    type PeerAddr;

    /// Find peers where shards of `vector_id` are located and can be retreived.
    fn shard_locations(&self, vector_id: &Self::DataIdentifier) -> Vec<Self::PeerAddr>;
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum Transaction<TOperandId, TOperandPieceId, TPeerId> {
    /// We want to put data at this (memory) address with specified distribution
    StorageRequest {
        address: TOperandId,
        distribution: Vec<(TPeerId, TOperandPieceId)>,
    },
    /// Indicates that specified piece (data) of operand is stored somewhere
    Stored(TOperandId, TOperandPieceId),
    /// Instruction is queued for execution by the author
    Execute(Instruction<TOperandId>),
}
