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
    instructions: Vec<Instruction<Vid, Vid>>,
    identifier: ProgramIdentifier,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
pub struct ProgramIdentifier {
    pub hash: Hash,
    pub event_hash: Hash,
}

impl Program {
    pub fn new(
        instructions: Vec<Instruction<Vid, Vid>>,
        event_hash: Hash,
    ) -> bincode::Result<Self> {
        let hash = Self::calculate_hash(&instructions)?;
        Ok(Self {
            instructions,
            identifier: ProgramIdentifier { hash, event_hash },
        })
    }

    fn instructions_digest(list: &Vec<Instruction<Vid, Vid>>) -> bincode::Result<Vec<u8>> {
        bincode::serialize(list)
    }

    fn calculate_hash(value: &Vec<Instruction<Vid, Vid>>) -> bincode::Result<Hash> {
        let mut hasher = Blake2b512::new();
        hasher.update(Self::instructions_digest(value)?);
        Ok(Hash::from_array(hasher.finalize().try_into().expect(
            "Fixed hash function must return same result length",
        )))
    }

    pub fn instructions(&self) -> &Vec<Instruction<Vid, Vid>> {
        &self.instructions
    }

    pub fn identifier(&self) -> &ProgramIdentifier {
        &self.identifier
    }

    pub fn into_parts(self) -> (Vec<Instruction<Vid, Vid>>, ProgramIdentifier) {
        (self.instructions, self.identifier)
    }
}

impl IntoIterator for Program {
    type Item = Instruction<Vid, Vid>;

    type IntoIter = <Vec<Instruction<Vid, Vid>> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.instructions.into_iter()
    }
}
