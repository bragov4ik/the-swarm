//! Consensus and its main functions.
//! todo: revisit docs
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

use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::processor::{Instructions, Program, ProgramIdentifier};

pub mod graph;

#[derive(Serialize, Deserialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
pub enum Transaction<TDataId, TShardId, TPeerId> {
    /// All data will be stored according to this distribution.
    /// Before this tx other ones are not processed
    InitializeStorage {
        distribution: Vec<(TPeerId, TShardId)>,
    },
    /// We want to put data at this (memory) address with distribution specified in
    /// `InitializeStorage` before
    StorageRequest { data_id: TDataId },
    /// Indicates that specified shard (data) of operand is stored somewhere
    Stored(TDataId, TShardId),
    /// Program is queued for execution by the author
    Execute(Instructions),
    /// Program was fully executed by this peer
    Executed(ProgramIdentifier),
}

impl<D: Debug, S, P> Transaction<D, S, P> {
    pub fn variant_short_string(&self) -> String {
        match self {
            Transaction::InitializeStorage { distribution: _ } => "InitializeStorage".to_owned(),
            Transaction::StorageRequest { data_id } => format!("StorageRequest({:?})", data_id),
            Transaction::Stored(id, _) => format!("Stored({:?})", id),
            Transaction::Execute(ins) => {
                let hash = Program::calculate_hash(ins).unwrap();
                format!("Execute({:?})", hash)
            }
            Transaction::Executed(id) => format!("Executed({:?})", id),
        }
    }
}
