use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Vid(pub u64);

// TODO: change to shard from ec lib
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Shard(pub u64);

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Graph {
    pub some_data: String,
}
