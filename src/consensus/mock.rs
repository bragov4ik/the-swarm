use std::collections::VecDeque;
use void::Void;

use super::{GraphConsensus, Transaction};

pub struct MockConsensus {
    transactions: VecDeque<Transaction<i32>>
}

impl GraphConsensus for MockConsensus {
    type Error = Void;
    type Transaction = Transaction<i32>;
    type Graph = ();

    fn update_graph(&mut self, _new_graph: Self::Graph) -> Result<(), Self::Error> {
        Ok(())
    }

    fn next_tx(&mut self) -> Option<Self::Transaction> {
        self.transactions.pop_back()
    }

    fn push_tx(&mut self, tx: Self::Transaction) -> Result<(), Self::Error> {
        Ok(self.transactions.push_front(tx))
    }
}