use serde::{Deserialize, Serialize};

macro_rules! impl_binary_constructor {
    ($function_name: ident, $enum_variant: ident) => {
        impl<TOperand, TResult> Instruction<TOperand, TResult> {
            pub fn $function_name(first: TOperand, second: TOperand, result: TResult) -> Self {
                Instruction {
                    operation: Operation::$enum_variant(BinaryOp { first, second }),
                    result,
                }
            }
        }
    };
}

macro_rules! impl_unary {
    ($function_name: ident, $enum_variant: ident) => {
        impl<TOperand, TResult> Instruction<TOperand, TResult> {
            pub fn $function_name(operand: TOperand, result: TResult) -> Self {
                Instruction {
                    operation: Operation::$enum_variant(UnaryOp { operand }),
                    result,
                }
            }
        }
    };
}

#[derive(Serialize, Deserialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
pub struct Instruction<TOperand, TResult> {
    pub operation: Operation<TOperand>,
    pub result: TResult,
}

impl<TOperand, TResult> Instruction<TOperand, TResult> {
    pub fn map_operands<F, TNewOperand>(self, f: F) -> Instruction<TNewOperand, TResult>
    where
        F: Fn(TOperand) -> TNewOperand,
    {
        let new_op = match self.operation {
            Operation::Dot(binary) => Operation::Dot(BinaryOp {
                first: f(binary.first),
                second: f(binary.second),
            }),
            Operation::Plus(binary) => Operation::Plus(BinaryOp {
                first: f(binary.first),
                second: f(binary.second),
            }),
            Operation::Inv(unary) => Operation::Inv(UnaryOp {
                operand: f(unary.operand),
            }),
        };
        Instruction {
            operation: new_op,
            result: self.result,
        }
    }

    pub fn as_ref(&self) -> Instruction<&TOperand, &TResult> {
        let ref_op = match self.operation {
            Operation::Dot(ref o) => Operation::Dot(o.as_ref()),
            Operation::Plus(ref o) => Operation::Plus(o.as_ref()),
            Operation::Inv(ref o) => Operation::Inv(o.as_ref()),
        };
        Instruction {
            operation: ref_op,
            result: &self.result,
        }
    }
}

impl<O, R> Instruction<Option<O>, R> {
    pub fn transpose_operation(self) -> Option<Instruction<O, R>> {
        match self.operation {
            Operation::Dot(ref o) => o.transpose().map(|o| Instruction {
                operation: Operation::Dot(o),
                result: self.result,
            }),
            Operation::Plus(ref o) => o.transpose().map(|o| Instruction {
                operation: Operation::Plus(o),
                result: self.result,
            }),
            Operation::Inv(ref o) => o.transpose().map(|o| Instruction {
                operation: Operation::Inv(o),
                result: self.result,
            }),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
pub enum Operation<TOperand> {
    Dot(BinaryOp<TOperand>),
    Plus(BinaryOp<TOperand>),
    Inv(UnaryOp<TOperand>),
}

impl_binary_constructor!(dot, Dot);
impl_binary_constructor!(plus, Plus);
impl_unary!(inv, Inv);

#[derive(Serialize, Deserialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
pub struct BinaryOp<TOperand> {
    pub first: TOperand,
    pub second: TOperand,
}

impl<O> BinaryOp<O> {
    pub fn as_ref(&self) -> BinaryOp<&O> {
        BinaryOp {
            first: &self.first,
            second: &self.second,
        }
    }
}

impl<O> BinaryOp<Option<O>> {
    pub fn transpose(self) -> Option<BinaryOp<O>> {
        match (self.first, self.second) {
            (Some(first), Some(second)) => Some(BinaryOp { first, second }),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, std::hash::Hash, Debug, Clone)]
pub struct UnaryOp<TOperand> {
    pub operand: TOperand,
}

impl<O> UnaryOp<O> {
    pub fn as_ref(&self) -> UnaryOp<&O> {
        UnaryOp {
            operand: &self.operand,
        }
    }
}

impl<O> UnaryOp<Option<O>> {
    pub fn transpose(self) -> Option<UnaryOp<O>> {
        match self.operand {
            Some(operand) => Some(UnaryOp { operand }),
            None => None,
        }
    }
}
