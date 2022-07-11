use void::Void;

use crate::types::{Shard, Vid};

use super::{Instruction, Processor};

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
    type Id = Vid;

    fn execute(ins: &Instruction<&Shard, &Vid>) -> Result<Shard, Self::Error> {
        // For demostrative purposes, let's have mathematical operations
        // And -> *
        // Or -> +
        // Not -> - (unary)
        let res = match *ins {
            Instruction::Dot(a, b, _) => map_zip(a, b, i32::saturating_mul),
            Instruction::Plus(a, b, _) => map_zip(a, b, i32::saturating_add),
            Instruction::Inv(a, _) => a.map(|n| -n),
        };
        Ok(res)
    }

    fn execute_batch(ins: &[Instruction<&Shard, &Vid>]) -> Vec<Result<Shard, Self::Error>> {
        ins.iter().map(Self::execute).collect()
    }
}
