use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;
use tokio::{
    join,
    sync::{mpsc, oneshot},
};

use crate::{
    encoding::{self},
    types::{Data, Hash, Shard, Sid, Vid},
};

use super::{instruction::Instruction, BinaryOp, Operation, Processor, Program, UnaryOp};

pub struct Module;

impl crate::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type SharedState = ModuleState;
}

pub enum OutEvent {
    FinishedExecution {
        program_hash: Hash,
        results: Vec<Result<Vid, Error>>,
    },
}

pub enum InEvent {
    Execute(Program),
}

pub enum ModuleState {
    Ready,
    Executing,
}

impl crate::State for ModuleState {
    fn accepts_input(&self) -> bool {
        match self {
            ModuleState::Ready => true,
            ModuleState::Executing => false,
        }
    }
}

pub struct Settings {
    pub data_shards_total: u64,
    pub data_shards_sufficient: u64,
}

struct MemoryBus {
    data_requester: mpsc::Sender<(Vid, oneshot::Sender<Shard>)>,
    data_writer: mpsc::Sender<(Vid, Shard)>,
}

impl MemoryBus {
    // ideally should just request data from local storage; not with currently used math
    async fn request_local_shard(&self, data_id: Vid) -> Result<oneshot::Receiver<Shard>, Error> {
        let (response_sender, response_reciever) = oneshot::channel();
        self.data_requester
            .send((data_id, response_sender))
            .await
            .map_err(|_| Error::DataRequestChannelClosed)?;
        Ok(response_reciever)
    }

    /// Assemble data necessary to complete the instruction(s)
    pub async fn retrieve_local_shard(&self, data_id: Vid) -> Result<Shard, Error> {
        let reciever = self.request_local_shard(data_id).await?;
        reciever.await.map_err(|_| Error::ResponseChannelClosed)
    }

    // todo: change to shard when switching encoding
    pub async fn store_local_shard(&self, data_id: Vid, shard: Shard) -> Result<(), Error> {
        self.data_writer
            .send((data_id, shard))
            .await
            .map_err(|_| Error::DataWriteChannelClosed)
    }
}

pub struct SimpleProcessor {
    memory_access: MemoryBus,
}
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

impl SimpleProcessor {
    fn calculate(operation: &Operation<Shard>) -> Shard {
        match operation {
            Operation::Dot(operation) => map_zip(
                &operation.first,
                &operation.second,
                reed_solomon_erasure::galois_8::mul,
            ),
            // todo: sub = add, div, other stuff from the lib
            Operation::Plus(operation) => map_zip(
                &operation.first,
                &operation.second,
                reed_solomon_erasure::galois_8::add,
            ),
            // inverses in GF(2^8) are the same values, because
            // the arithmetic is done on polynomials over GF(2)
            // and addition of any coefficient on itself gives 0
            // in GF(2)
            Operation::Inv(operation) => operation.operand.map(|n| n),
        }
    }

    async fn retrieve_operand(
        &self,
        operand: Vid,
        operand_context: Option<Shard>,
    ) -> Result<Shard, Error> {
        if let Some(o) = operand_context {
            return Ok(o);
        }
        self.memory_access.retrieve_local_shard(operand).await
    }

    async fn retrieve_binary(
        &self,
        binary: BinaryOp<Vid>,
        context: &HashMap<Vid, Shard>,
    ) -> Result<BinaryOp<Shard>, Error> {
        let BinaryOp { first, second } = binary;
        let first_cx = context.get(&first).cloned();
        let second_cx = context.get(&second).cloned();
        let (first, second) = join!(
            self.retrieve_operand(first, first_cx),
            self.retrieve_operand(second, second_cx)
        );
        Ok(BinaryOp {
            first: first?,
            second: second?,
        })
    }

    async fn retrieve_unary(
        &self,
        unary: UnaryOp<Vid>,
        context: &HashMap<Vid, Shard>,
    ) -> Result<UnaryOp<Shard>, Error> {
        let operand = unary.operand;
        let cx = context.get(&operand);
        let operand = self.retrieve_operand(operand, cx.cloned()).await?;
        Ok(UnaryOp { operand })
    }

    async fn retrieve_operands(
        &self,
        op: Operation<Vid>,
        context: &HashMap<Vid, Shard>,
    ) -> Result<Operation<Shard>, Error> {
        let retrieved = match op {
            Operation::Dot(binary) => Operation::Dot(self.retrieve_binary(binary, context).await?),
            Operation::Plus(binary) => {
                Operation::Plus(self.retrieve_binary(binary, context).await?)
            }
            Operation::Inv(unary) => Operation::Inv(self.retrieve_unary(unary, context).await?),
        };
        Ok(retrieved)
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Channel to memory for data requests was closed.")]
    DataRequestChannelClosed,
    #[error("Channel to memory for data writes was closed.")]
    DataWriteChannelClosed,
    #[error("Channel for getting response from memory bus was closed")]
    ResponseChannelClosed,
    #[error(transparent)]
    EncodingError(#[from] encoding::mock::Error),
}

#[async_trait]
impl Processor<Program> for SimpleProcessor {
    type Error = Error;
    type Operand = Vid;
    type Result = Vid;

    async fn execute_one(
        &self,
        ins: Instruction<Self::Operand, Self::Result>,
    ) -> Result<Self::Operand, Self::Error> {
        self.execute(vec![ins].into()).await.remove(0)
    }
    async fn execute(&self, program: Program) -> Vec<Result<Self::Operand, Self::Error>> {
        let mut context = HashMap::new();
        let mut results = Vec::with_capacity(program.instructions().len());
        for instruction in program {
            let Instruction {
                operation,
                result: result_id,
            } = instruction;
            let operation = match self.retrieve_operands(operation, &context).await {
                Ok(o) => o,
                Err(e) => {
                    results.push(Err(e));
                    continue;
                }
            };
            let output = Self::calculate(&operation);
            context.insert(result_id.clone(), output.clone());
            if let Err(e) = self
                .memory_access
                .store_local_shard(result_id.clone(), output)
                .await
            {
                results.push(Err(e));
                continue;
            };
            results.push(Ok(result_id));
        }
        results
    }
}
