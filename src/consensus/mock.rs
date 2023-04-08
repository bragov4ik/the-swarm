//! Mock implementation of consensus. Not sure if needed, so unchanged for now.
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
};
use tracing::debug;

use super::{DataDiscoverer, GraphConsensus, Transaction};
use crate::{instruction_memory::InstructionMemory, processor::Instruction};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct MockConsensus<TOperandId: Hash + Eq> {
    instructions: Vec<Instruction<TOperandId, TOperandId>>,
    #[serde(skip)]
    instruction_pointer: usize,
    data_locations: HashMap<TOperandId, PeerId>,
    version: u64,
    self_id: PeerId,
}

impl<TOperand: Hash + Eq> MockConsensus<TOperand> {
    pub fn new(self_id: PeerId) -> Self {
        MockConsensus {
            instructions: Vec::new(),
            instruction_pointer: 0,
            data_locations: HashMap::new(),
            version: 0,
            self_id,
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

impl<TOperandId: Clone + Hash + Eq> InstructionMemory for MockConsensus<TOperandId> {
    type Error = Error;
    type Instruction = Instruction<TOperandId, TOperandId>;

    fn push_instruction(&mut self, instruction: Self::Instruction) -> Result<(), Self::Error> {
        self.push_tx(Transaction::Execute(instruction))
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
    type OperandId = TOperand;
    type OperandPieceId = ();
    type PeerId = PeerId;
    type SyncPayload = Self;

    type UpdateError = Error;
    type PushTxError = Error;

    fn update_graph(&mut self, new_graph: Self::SyncPayload) -> Result<(), Error> {
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

    fn get_sync(&self, _sync_for: &Self::PeerId) -> Self::SyncPayload {
        self.clone()
    }

    fn push_tx(
        &mut self,
        tx: Transaction<Self::OperandId, Self::OperandPieceId, Self::PeerId>,
    ) -> Result<(), Error> {
        match tx {
            Transaction::Execute(i) => {
                self.instructions.push(i);
                self.version += 1;
                Ok(())
            }
            Transaction::Stored(vid, _piece_id) => match self.data_locations.entry(vid) {
                Entry::Occupied(_) => Err(Error::VidInUse),
                Entry::Vacant(e) => {
                    e.insert(self.self_id);
                    self.version += 1;
                    Ok(())
                }
            },
            // todo: check if needed
            Transaction::StorageRequest {
                address,
                distribution,
            } => Ok(()),
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
