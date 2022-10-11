use std::fmt::Debug;

use serde::{Deserialize, Serialize};

pub(crate) mod mock;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum Instruction<OP, ID> {
    // First operands, then id of result
    Dot(OP, OP, ID),
    Plus(OP, OP, ID),
    Inv(OP, ID),
}

impl<OP, ID> Instruction<OP, ID> {
    pub fn get_dest(&self) -> &ID {
        match self {
            Instruction::Dot(_, _, id) => id,
            Instruction::Plus(_, _, id) => id,
            Instruction::Inv(_, id) => id,
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
