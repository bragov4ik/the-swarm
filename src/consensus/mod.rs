use serde::{Deserialize, Serialize};

use crate::{types::Vid, processor::Instruction, instruction_memory::InstructionMemory};

pub(crate) mod mock;

pub trait GraphConsensus: InstructionMemory<Instruction = Instruction<Self::Operator>> {
    type Operator;
    type Graph;

    fn update_graph(&mut self, new_graph: Self::Graph) -> Result<(), Self::Error>;
    fn push_tx(&mut self, tx: Transaction<Self::Operator>) -> Result<(), Self::Error>;
    fn next_tx(&mut self) -> Option<Transaction<Self::Operator>>;
}

pub trait DataDiscoverer {
    type DataIdentifier;
    type PeerAddr;

    fn shard_locations(&self, vector_id: Self::DataIdentifier) -> Option<Vec<Self::PeerAddr>>;
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Transaction<OP> {
    /// Indicates that author stores shard of vector with id `Vid`
    Stored(Vid),
    /// Instruction is queued for execution by the author
    ExecutionRequest(Instruction<OP>),
}
