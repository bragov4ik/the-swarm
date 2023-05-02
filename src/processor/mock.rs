use void::Void;

use crate::types::{Shard};

use super::{Instruction, Processor, instruction::Operation};

pub struct MockProcessor {}

fn map_zip<T, const N: usize, F>(a: &[T; N], b: &[T; N], f: F) -> [T; N]
where
    T: Clone,
    F: Fn(T, T) -> T,
{
    let mut result = a.clone();
    for (r, b_item) in result.iter_mut().zip(b.iter()) {
        *r = f(r.clone(), b_item.clone());
    }
    result
}

impl Processor for MockProcessor {
    type Error = Void;
    type Operand = Shard;

    fn execute_one<R>(ins: &Instruction<&Shard, R>) -> Result<Shard, Self::Error> {
        // For demostrative purposes, let's have mathematical operations
        // And -> *
        // Or -> +
        // Not -> - (unary)
        let res = match ins.operation {
            Operation::Dot(operation) => {
                map_zip(operation.first, operation.second, i32::saturating_mul)
            }
            Operation::Plus(operation) => {
                map_zip(operation.first, operation.second, i32::saturating_add)
            }
            Operation::Inv(operation) => operation.operand.map(|n| -n),
        };
        Ok(res)
    }

    fn execute<R>(ins: &[Instruction<&Shard, R>]) -> Vec<Result<Shard, Self::Error>> {
        ins.iter().map(Self::execute_one).collect()
    }
}
