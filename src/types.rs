use serde::{Deserialize, Serialize};

pub struct Operand {}

pub enum Instruction {
    And(Operand, Operand),
    Or(Operand, Operand),
    Xor(Operand, Operand),
    Not(Operand),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Vid(pub u64);

// TODO: change to shard from ec lib
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Shard(pub u64);

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Graph {
    pub some_data: String,
}
