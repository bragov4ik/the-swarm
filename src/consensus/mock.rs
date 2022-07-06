use std::collections::VecDeque;
use void::Void;

use crate::{instruction_memory::InstructionMemory, processor::Instruction};

use super::{GraphConsensus, Transaction};

pub struct MockConsensus<OP> {
    transactions: VecDeque<Transaction<OP>>
}

impl<OP> InstructionMemory for MockConsensus<OP> {
    type Error = Void;
    type Instruction = Instruction<OP>;

    fn push_instruction(&mut self, instruction: Self::Instruction) -> Result<(), Self::Error> {
        self.push_tx(Transaction::ExecutionRequest(instruction))
    }

    fn next_instruction(&mut self) -> Option<Self::Instruction> {
        while let Some(tx) = self.next_tx() {
            match tx {
                Transaction::Stored(_) => {},
                Transaction::ExecutionRequest(ins) => return Some(ins),
            }
        }
        None
    }
}

impl<OP> GraphConsensus for MockConsensus<OP> {
    type Operator = OP;
    type Graph = ();

    fn update_graph(&mut self, _new_graph: Self::Graph) -> Result<(), Self::Error> {
        Ok(())
    }

    fn next_tx(&mut self) -> Option<Transaction<Self::Operator>> {
        self.transactions.pop_back()
    }

    fn push_tx(&mut self, tx: Transaction<Self::Operator>) -> Result<(), Self::Error> {
        Ok(self.transactions.push_front(tx))
    }
}