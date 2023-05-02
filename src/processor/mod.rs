use std::fmt::Debug;

use async_trait::async_trait;

pub use self::instruction::{BinaryOp, Instruction, Operation, UnaryOp};

// pub(crate) mod mock;
pub mod instruction;
pub mod single_threaded;

#[async_trait]
pub trait Processor<TProgram>
where
    TProgram: IntoIterator<Item = Instruction<Self::Operand, Self::Result>>,
{
    type Error: Debug;
    type Operand: Eq;
    type Result;

    async fn execute_one(
        &self,
        ins: Instruction<Self::Operand, Self::Result>,
    ) -> Result<Self::Operand, Self::Error>;
    async fn execute(&self, program: TProgram) -> Vec<Result<Self::Operand, Self::Error>>;
}
