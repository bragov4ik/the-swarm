use void::Void;

use crate::types::{Shard, Vid};

use super::{Instruction, Processor};

pub struct MockProcessor {}

impl Processor for MockProcessor {
    type Error = Void;
    type Operand = Shard;
    type Id = Vid;

    fn execute(ins: &Instruction<&Shard, &Vid>) -> Result<Shard, Self::Error> {
        // For demostrative purposes, let's have mathematical operations
        // And -> *
        // Or -> +
        // Not -> - (unary)
        let res = match *ins {
            Instruction::And(a, b, _) => a * b,
            Instruction::Or(a, b, _) => a + b,
            Instruction::Not(a, _) => -a,
        };
        Ok(res)
    }

    fn execute_batch(ins: &[Instruction<&Shard, &Vid>]) -> Vec<Result<Shard, Self::Error>> {
        ins.iter().map(Self::execute).collect()
    }
}
