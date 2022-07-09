use serde::{Deserialize, Serialize};

use crate::consensus::mock::MockConsensus;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Hash)]
pub struct Vid(pub u64);

// TODO: change to actual data shard
// #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub type Shard = i32;

pub type Graph = MockConsensus<Vid>;
