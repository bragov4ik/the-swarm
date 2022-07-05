use void::Void;

use super::{Processor, Instruction};

pub struct MockProcessor {}

impl Processor for MockProcessor {
    type Error = Void;
    type Operand = i32;

    fn execute(ins: &Instruction<i32>) -> Result<i32, Self::Error> {
        // For demostrative purposes, let's have mathematical operations
        // And -> *
        // Or -> +
        // Not -> - (unary)
        let res = match ins {
            Instruction::And(a, b) => a * b,
            Instruction::Or(a, b) => a + b,
            Instruction::Not(a) => -a,
        };
        Ok(res)
    }

    fn execute_batch(ins: &[Instruction<i32>]) -> Vec<Result<i32, Self::Error>> {
        ins.iter().map(Self::execute).collect()
    }
}