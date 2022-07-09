use std::fmt::Debug;

use serde::{Deserialize, Serialize};

pub(crate) mod mock;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Instruction<OP, ID> {
    // First operands, then id of result
    And(OP, OP, ID),
    Or(OP, OP, ID),
    Not(OP, ID),
}

impl<OP, ID> Instruction<OP, ID> {
    pub fn get_dest(&self) -> &ID {
        match self {
            Instruction::And(_, _, id) => id,
            Instruction::Or(_, _, id) => id,
            Instruction::Not(_, id) => id,
        }
    }
}

pub trait Processor {
    type Error: Debug;
    type Operand: Eq;
    type Id: Eq;

    fn execute(ins: &Instruction<&Self::Operand, &Self::Id>) -> Result<Self::Operand, Self::Error>;
    fn execute_batch(
        ins: &[Instruction<&Self::Operand, &Self::Id>],
    ) -> Vec<Result<Self::Operand, Self::Error>>;
}
