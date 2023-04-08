use std::fmt::Debug;

use serde::{Deserialize, Serialize};

pub(crate) mod mock;

macro_rules! impl_binary {
    ($function_name: ident, $enum_variant: ident) => {
        impl<TOperand> Instruction<TOperand> {
            pub fn $function_name(first: TOperand, second: TOperand, result: TOperand) -> Self {
                Instruction::$enum_variant(BinaryOp {
                    first,
                    second,
                    result,
                })
            }
        }
    };
}

macro_rules! impl_unary {
    ($function_name: ident, $enum_variant: ident) => {
        impl<TOperand> Instruction<TOperand> {
            pub fn $function_name(first: TOperand, result: TOperand) -> Self {
                Instruction::$enum_variant(UnaryOp { first, result })
            }
        }
    };
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum Instruction<TOperand> {
    // First operands, then result
    Dot(BinaryOp<TOperand>),
    Plus(BinaryOp<TOperand>),
    Inv(UnaryOp<TOperand>),
}

impl_binary!(dot, Dot);
impl_binary!(plus, Plus);
impl_unary!(inv, Inv);

pub struct BinaryOp<TOperand> {
    pub first: TOperand,
    pub second: TOperand,
    pub result: TOperand,
}

pub struct UnaryOp<TOperand> {
    pub first: TOperand,
    pub result: TOperand,
}

impl<TOperand> Instruction<TOperand> {
    pub fn destination(&self) -> &TOperand {
        match self {
            Instruction::Dot(dot) => &dot.result,
            Instruction::Plus(plus) => &plus.result,
            Instruction::Inv(inv) => &inv.result,
        }
    }
}

pub trait Processor {
    type Error: Debug;
    type Operand: Eq;

    fn execute(ins: &Instruction<&Self::Operand>) -> Result<Self::Operand, Self::Error>;
    fn execute_batch(
        ins: &[Instruction<&Self::Operand>],
    ) -> Vec<Result<Self::Operand, Self::Error>>;
}
