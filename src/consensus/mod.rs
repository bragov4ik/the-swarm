use crate::{types::Vid, processor::Instruction};

mod mock;

pub trait GraphConsensus {
    type Error;
    type Transaction;
    type Graph;

    fn update_graph(&mut self, new_graph: Self::Graph) -> Result<(), Self::Error>;
    fn next_tx(&mut self) -> Option<Self::Transaction>;
    fn push_tx(&mut self, tx: Self::Transaction) -> Result<(), Self::Error>;
}

pub trait DataDiscoverer {
    type DataIdentifier;
    type PeerAddr;

    fn shard_locations(&self, vector_id: Self::DataIdentifier) -> Option<Vec<Self::PeerAddr>>;
}

pub enum Transaction<OP> {
    /// Indicates that author stores shard of vector with id `Vid`
    Stored(Vid),
    /// Instruction is queued for execution by the author
    ExecutionRequest(Instruction<OP>),
}
