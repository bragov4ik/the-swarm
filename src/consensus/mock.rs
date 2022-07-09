use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::{hash_map::Entry, HashMap};

use crate::{instruction_memory::InstructionMemory, processor::Instruction, types::Vid};

use super::{DataDiscoverer, GraphConsensus, Transaction};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct MockConsensus<OP> {
    instructions: Vec<Instruction<OP, Vid>>,
    #[serde(skip)]
    instruction_pointer: usize,
    vec_locations: HashMap<Vid, PeerId>,
    version: u64,
}

impl<OP> MockConsensus<OP> {
    pub fn new() -> Self {
        MockConsensus {
            instructions: Vec::new(),
            instruction_pointer: 0,
            vec_locations: HashMap::new(),
            version: 0,
        }
    }
}

#[derive(Debug)]
pub enum Error {
    /// Vector with such ID already exists
    VidInUse,
    /// Tried to update the graph with not newer version
    VersionTooOld,
}

impl<OP: Clone> InstructionMemory for MockConsensus<OP> {
    type Error = Error;
    type Instruction = Instruction<OP, Vid>;

    fn push_instruction(&mut self, instruction: Self::Instruction) -> Result<(), Self::Error> {
        self.push_tx(Transaction::ExecutionRequest(instruction))
    }

    fn next_instruction(&mut self) -> Option<Self::Instruction> {
        let res = self.instructions.get(self.instruction_pointer).cloned();
        if let Some(_) = res {
            self.instruction_pointer += 1;
        }
        res
    }
}

impl<OP: Clone> GraphConsensus for MockConsensus<OP> {
    type Operator = OP;
    type Graph = Self;

    fn update_graph(&mut self, new_graph: Self::Graph) -> Result<(), Self::Error> {
        if new_graph.version > self.version {
            self.instructions
                .extend_from_slice(&new_graph.instructions[self.instructions.len()..]);
            self.vec_locations = new_graph.vec_locations;
            self.version = new_graph.version;
            Ok(())
        } else {
            Err(Error::VersionTooOld)
        }
    }

    fn get_graph(&self) -> Self::Graph {
        self.clone()
    }

    fn push_tx(&mut self, tx: Transaction<Self::Operator>) -> Result<(), Self::Error> {
        match tx {
            Transaction::ExecutionRequest(i) => {
                self.instructions.push(i);
                self.version += 1;
                Ok(())
            }
            Transaction::Stored(vid, pid) => match self.vec_locations.entry(vid) {
                Entry::Occupied(_) => Err(Error::VidInUse),
                Entry::Vacant(e) => {
                    e.insert(pid);
                    self.version += 1;
                    Ok(())
                }
            },
        }
    }
}

impl<OP> DataDiscoverer for MockConsensus<OP> {
    type DataIdentifier = Vid;
    type PeerAddr = PeerId;

    /// Returns places where the whole vector can be found. Should be 1 location max
    /// in the mock.
    fn shard_locations(&self, vector_id: &Self::DataIdentifier) -> Vec<Self::PeerAddr> {
        self.vec_locations
            .get(vector_id)
            .into_iter()
            .cloned()
            .collect()
    }
}
