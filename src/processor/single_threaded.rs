use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;
use tokio::{join, sync::mpsc};

use crate::{
    encoding::{self, mock::MockEncoding, DataEncoding},
    types::{Data, Shard, Vid},
};

use super::{instruction::Instruction, BinaryOp, Operation, Processor, UnaryOp};

pub struct Module;

impl crate::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type State = State;
}

pub enum OutEvent {
    FinishedExecution { results: Vec<Result<Vid, Error>> },
}

pub enum InEvent {
    Execute(Program),
}

pub enum State {
    Ready,
    Executing,
}

impl crate::State for State {
    fn accepts_input(&self) -> bool {
        match self {
            State::Ready => true,
            State::Executing => false,
        }
    }
}

pub struct Settings {
    pub data_pieces_total: u64,
    pub data_pieces_sufficient: u64,
}

struct MemoryBus {
    data_requester: mpsc::Sender<(Vid, mpsc::Sender<Shard>)>,
    data_writer: mpsc::Sender<(Vid, Vec<Shard>)>,
    settings: Settings,
}

impl MemoryBus {
    // ideally should just request data from local storage; not with currently used math
    async fn request_data(&self, data_id: Vid) -> Result<mpsc::Receiver<Shard>, Error> {
        let (response_sender, response_reciever) = mpsc::channel(
            self.settings
                .data_pieces_total
                .try_into()
                .expect("total data pieces # should fit into usize"),
        );
        self.data_requester
            .send((data_id, response_sender))
            .await
            .map_err(|_| Error::DataRequestChannelClosed)?;
        Ok(response_reciever)
    }

    /// Assemble data necessary to complete the instruction(s)
    pub async fn retrieve_data(&self, data_id: Vid) -> Result<Vec<Shard>, Error> {
        let mut reciever = self.request_data(data_id).await?;
        let sufficient_pieces = self
            .settings
            .data_pieces_sufficient
            .try_into()
            .expect("minimum data pieces # should fit into usize");
        let mut data = Vec::with_capacity(sufficient_pieces);
        while let Some(shard) = reciever.recv().await {
            data.push(shard);
            if data.len() >= sufficient_pieces {
                return Ok(data);
            }
        }
        return Err(Error::EncodingError(encoding::mock::Error::NotEnoughShards));
    }

    pub async fn store_local(&self, data_id: Vid, shards: Vec<Shard>) -> Result<(), Error> {
        self.data_writer
            .send((data_id, shards))
            .await
            .map_err(|_| Error::DataWriteChannelClosed)
    }
}

pub struct SimpleProcessor {
    settings: Settings,
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
    fn calculate(operation: &Operation<Data>) -> Data {
        match operation {
            Operation::Dot(operation) => {
                map_zip(&operation.first, &operation.second, i32::saturating_mul)
            }
            Operation::Plus(operation) => {
                map_zip(&operation.first, &operation.second, i32::saturating_add)
            }
            Operation::Inv(operation) => operation.operand.map(|n| -n),
        }
    }

    async fn retrieve_operand(
        &self,
        operand: Vid,
        operand_context: Option<Data>,
    ) -> Result<Data, Error> {
        if let Some(o) = operand_context {
            return Ok(o);
        }
        let shards = self.memory_access.retrieve_data(operand).await?;
        Ok(encoding::mock::MockEncoding::decode(shards)?)
    }

    async fn retrieve_binary(
        &self,
        binary: BinaryOp<Vid>,
        context: &HashMap<Vid, Data>,
    ) -> Result<BinaryOp<Data>, Error> {
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
        context: &HashMap<Vid, Data>,
    ) -> Result<UnaryOp<Data>, Error> {
        let operand = unary.operand;
        let cx = context.get(&operand);
        let operand = self.retrieve_operand(operand, cx.cloned()).await?;
        Ok(UnaryOp { operand })
    }

    async fn retrieve_operands(
        &self,
        op: Operation<Vid>,
        context: &HashMap<Vid, Data>,
    ) -> Result<Operation<Data>, Error> {
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
    #[error(transparent)]
    EncodingError(#[from] encoding::mock::Error),
}

pub type Program = Vec<Instruction<Vid, Vid>>;

#[async_trait]
impl Processor<Program> for SimpleProcessor {
    type Error = Error;
    type Operand = Vid;
    type Result = Vid;

    async fn execute_one(
        &self,
        ins: Instruction<Self::Operand, Self::Result>,
    ) -> Result<Self::Operand, Self::Error> {
        self.execute(vec![ins]).await.remove(0)
    }
    async fn execute(&self, program: Program) -> Vec<Result<Self::Operand, Self::Error>> {
        let mut context = HashMap::new();
        let mut results = Vec::with_capacity(program.len());
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
            let shards = match MockEncoding::encode(output) {
                Ok(s) => s,
                Err(e) => {
                    results.push(Err(e.into()));
                    continue;
                }
            };
            self.memory_access
                .store_local(result_id.clone(), shards)
                .await;
            results.push(Ok(result_id));
        }
        results
    }
}
