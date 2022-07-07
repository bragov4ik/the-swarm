use serde::{Serialize, Deserialize};



pub(crate) mod mock;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Instruction<OP> {
    And(OP, OP),
    Or(OP, OP),
    Not(OP),
}

pub trait Processor {
    type Error;
    type Operand: Eq;

    fn execute(ins: &Instruction<Self::Operand>) -> Result<Self::Operand, Self::Error>;
    fn execute_batch(ins: &[Instruction<Self::Operand>]) -> Vec<Result<Self::Operand, Self::Error>>;
}