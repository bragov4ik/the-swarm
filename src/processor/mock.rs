use void::Void;

use crate::types::Vid;

use super::{Instruction, Processor};

pub struct MockProcessor {}

impl Processor for MockProcessor {
    type Error = Void;
    type Operand = i32;
    type Id = Vid;

    fn execute(ins: &Instruction<&i32, &Vid>) -> Result<i32, Self::Error> {
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

    fn execute_batch(ins: &[Instruction<&i32, &Vid>]) -> Vec<Result<i32, Self::Error>> {
        ins.iter().map(Self::execute).collect()
    }
}
