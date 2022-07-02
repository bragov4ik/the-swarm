use crate::types::{Instruction, Operand};

pub trait Processor {
    type Error;
    fn execute(ins: Instruction) -> Result<Operand, Self::Error>;
    fn execute_batch(ins: &[Instruction]) -> Vec<Result<Operand, Self::Error>>;
}
