pub trait Consensus {
    type Error;
    type PeerAddr;
    type Instruction;
    type DataIdentifier;
    type Graph;

    fn update_graph(&mut self, new_graph: Self::Graph) -> Result<(), Self::Error>;
    fn next_instruction(&mut self) -> Option<Self::Instruction>;
    fn shard_locations(&self, vector_id: Self::DataIdentifier) -> Option<Vec<Self::PeerAddr>>;
    fn put_instruction(&mut self, instruction: Self::Instruction) -> Result<(), Self::Error>;
}
