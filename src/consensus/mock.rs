use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::{collections::{hash_map::Entry, HashMap}, hash::Hash};
use tracing::debug;

use crate::{instruction_memory::InstructionMemory, processor::Instruction};

use super::{DataDiscoverer, GraphConsensus, Transaction};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct MockConsensus<TOperand: Hash + Eq> {
    instructions: Vec<Instruction<TOperand, TOperand>>,
    #[serde(skip)]
    instruction_pointer: usize,
    data_locations: HashMap<TOperand, PeerId>,
    version: u64,
}

impl<TOperand: Hash + Eq> MockConsensus<TOperand> {
    pub fn new() -> Self {
        MockConsensus {
            instructions: Vec::new(),
            instruction_pointer: 0,
            data_locations: HashMap::new(),
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

impl<TOperand: Clone + Hash + Eq> InstructionMemory for MockConsensus<TOperand> {
    type Error = Error;
    type Instruction = Instruction<TOperand, TOperand>;

    fn push_instruction(&mut self, instruction: Self::Instruction) -> Result<(), Self::Error> {
        self.push_tx(Transaction::ExecutionRequest(instruction))
    }

    fn next_instruction(&mut self) -> Option<Self::Instruction> {
        let res = self.instructions.get(self.instruction_pointer).cloned();
        if res.is_some() {
            self.instruction_pointer += 1;
        }
        res
    }
}

impl<TOperand: Clone + Hash + Eq> GraphConsensus for MockConsensus<TOperand> {
    type Operand = TOperand;
    type Location = PeerId;
    type SyncPayload = Self;

    fn update_graph(&mut self, new_graph: Self::SyncPayload) -> Result<(), Self::Error> {
        if new_graph.version > self.version {
            debug!(
                "Received version is newer ({}>{}), updating state",
                new_graph.version, self.version
            );
            self.instructions
                .extend_from_slice(&new_graph.instructions[self.instructions.len()..]);
            self.data_locations = new_graph.data_locations;
            self.version = new_graph.version;
            Ok(())
        } else {
            Err(Error::VersionTooOld)
        }
    }

    fn get_graph(&self) -> Self::SyncPayload {
        self.clone()
    }

    fn push_tx(&mut self, tx: Transaction<Self::Operand, Self::Location>) -> Result<(), Self::Error> {
        match tx {
            Transaction::ExecutionRequest(i) => {
                self.instructions.push(i);
                self.version += 1;
                Ok(())
            }
            Transaction::Stored(vid, pid) => match self.data_locations.entry(vid) {
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

impl<TOperand: Hash + Eq> DataDiscoverer for MockConsensus<TOperand> {
    type DataIdentifier = TOperand;
    type PeerAddr = PeerId;

    /// Returns places where the whole vector can be found. Should be 1 location max
    /// in the mock.
    fn shard_locations(&self, vector_id: &Self::DataIdentifier) -> Vec<Self::PeerAddr> {
        self.data_locations
            .get(vector_id)
            .into_iter()
            .cloned()
            .collect()
    }
}
