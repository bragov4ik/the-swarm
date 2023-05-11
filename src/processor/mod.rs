use std::fmt::Debug;

use async_trait::async_trait;
use blake2::{Blake2b512, Digest};
use serde::{Deserialize, Serialize};

pub use self::instruction::{BinaryOp, Instruction, Operation, UnaryOp};
use crate::types::{Hash, Vid};

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

#[derive(Serialize, Deserialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
pub struct Program {
    instructions: Instructions,
    identifier: ProgramIdentifier,
}

pub type Instructions = Vec<Instruction<Vid, Vid>>;

#[derive(Serialize, Deserialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
pub struct ProgramIdentifier {
    pub hash: Hash,
    pub event_hash: Hash,
}

impl Program {
    pub fn new(instructions: Instructions, event_hash: Hash) -> bincode::Result<Self> {
        let hash = Self::calculate_hash(&instructions)?;
        Ok(Self {
            instructions,
            identifier: ProgramIdentifier { hash, event_hash },
        })
    }

    fn instructions_digest(list: &Instructions) -> bincode::Result<Vec<u8>> {
        bincode::serialize(list)
    }

    fn calculate_hash(value: &Instructions) -> bincode::Result<Hash> {
        let mut hasher = Blake2b512::new();
        hasher.update(Self::instructions_digest(value)?);
        Ok(Hash::from_array(hasher.finalize().try_into().expect(
            "Fixed hash function must return same result length",
        )))
    }

    pub fn instructions(&self) -> &Instructions {
        &self.instructions
    }

    pub fn identifier(&self) -> &ProgramIdentifier {
        &self.identifier
    }

    pub fn into_parts(self) -> (Instructions, ProgramIdentifier) {
        (self.instructions, self.identifier)
    }
}

impl IntoIterator for Program {
    type Item = Instruction<Vid, Vid>;

    type IntoIter = <Instructions as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.instructions.into_iter()
    }
}
