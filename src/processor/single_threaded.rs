use std::collections::HashMap;

use thiserror::Error;
use tokio::{
    join,
    sync::{mpsc, oneshot},
};
use tracing::{error, info, warn};

use crate::module::ModuleChannelServer;
use crate::{
    encoding::{self},
    types::{Shard, Vid},
};

use super::{
    instruction::Instruction, BinaryOp, Instructions, Operation, Program, ProgramIdentifier,
    UnaryOp,
};

pub struct Module;

impl crate::module::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type SharedState = ModuleState;
}

#[derive(Debug, Clone)]
pub enum OutEvent {
    FinishedExecution {
        program_id: ProgramIdentifier,
        results: Vec<Result<Vid, Error>>,
    },
}

#[derive(Debug, Clone)]
pub enum InEvent {
    Execute(Program),
}

pub enum ModuleState {
    Ready,
    Executing,
}

impl crate::module::State for ModuleState {
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

pub struct MemoryBus {
    reads: mpsc::Sender<(Vid, oneshot::Sender<Option<Shard>>)>,
    writes: mpsc::Sender<(Vid, Shard)>,
}

impl MemoryBus {
    pub fn new(
        reads: mpsc::Sender<(Vid, oneshot::Sender<Option<Shard>>)>,
        writes: mpsc::Sender<(Vid, Shard)>,
    ) -> Self {
        Self { reads, writes }
    }

    #[allow(unused)]
    pub fn channel(buffer: usize) -> (crate::data_memory::distributed_simple::MemoryBus, Self) {
        crate::data_memory::distributed_simple::MemoryBus::channel(buffer)
    }
}

impl MemoryBus {
    // ideally should just request data from local storage; not with currently used math
    async fn request_local_shard(
        &self,
        data_id: Vid,
    ) -> Result<oneshot::Receiver<Option<Shard>>, Error> {
        let (response_sender, response_reciever) = oneshot::channel();
        self.reads
            .send((data_id, response_sender))
            .await
            .map_err(|_| Error::DataRequestChannelClosed)?;
        Ok(response_reciever)
    }

    /// Assemble data necessary to complete the instruction(s)
    pub async fn retrieve_local_shard(&self, data_id: Vid) -> Result<Option<Shard>, Error> {
        let reciever = self.request_local_shard(data_id).await?;
        reciever.await.map_err(|_| Error::ResponseChannelClosed)
    }

    // todo: change to shard when switching encoding
    pub async fn store_local_shard(&self, data_id: Vid, shard: Shard) -> Result<(), Error> {
        self.writes
            .send((data_id, shard))
            .await
            .map_err(|_| Error::DataWriteChannelClosed)
    }
}

pub struct ShardProcessor {
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

impl ShardProcessor {
    fn calculate(operation: &Operation<Shard>) -> Shard {
        match operation {
            Operation::Sub(operation) => map_zip(
                &operation.first,
                &operation.second,
                reed_solomon_erasure::galois_8::add,
            ),
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
    ) -> Result<Option<Shard>, Error> {
        if let Some(o) = operand_context {
            return Ok(Some(o));
        }
        self.memory_access.retrieve_local_shard(operand).await
    }

    async fn retrieve_binary(
        &self,
        binary: BinaryOp<Vid>,
        context: &HashMap<Vid, Shard>,
    ) -> Result<Option<BinaryOp<Shard>>, Error> {
        let BinaryOp { first, second } = binary;
        let first_cx = context.get(&first).cloned();
        let second_cx = context.get(&second).cloned();
        let (first, second) = join!(
            self.retrieve_operand(first, first_cx),
            self.retrieve_operand(second, second_cx)
        );
        let operation = if let (Some(first), Some(second)) = (first?, second?) {
            Some(BinaryOp { first, second })
        } else {
            None
        };
        Ok(operation)
    }

    async fn retrieve_unary(
        &self,
        unary: UnaryOp<Vid>,
        context: &HashMap<Vid, Shard>,
    ) -> Result<Option<UnaryOp<Shard>>, Error> {
        let operand = unary.operand;
        let cx = context.get(&operand);
        let Some(operand) = self.retrieve_operand(operand, cx.cloned()).await? else {
            return Ok(None);
        };
        Ok(Some(UnaryOp { operand }))
    }

    async fn retrieve_operands(
        &self,
        op: Operation<Vid>,
        context: &HashMap<Vid, Shard>,
    ) -> Result<Operation<Shard>, Error> {
        let retrieved = match op {
            Operation::Sub(binary) => Operation::Sub(
                self.retrieve_binary(binary, context)
                    .await?
                    .ok_or(Error::NoShardsAssigned)?,
            ),
            Operation::Plus(binary) => Operation::Plus(
                self.retrieve_binary(binary, context)
                    .await?
                    .ok_or(Error::NoShardsAssigned)?,
            ),
            Operation::Inv(unary) => Operation::Inv(
                self.retrieve_unary(unary, context)
                    .await?
                    .ok_or(Error::NoShardsAssigned)?,
            ),
        };
        Ok(retrieved)
    }
}

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Channel to memory for data requests was closed.")]
    DataRequestChannelClosed,
    #[error("Channel to memory for data writes was closed.")]
    DataWriteChannelClosed,
    #[error("Channel for getting response from memory bus was closed")]
    ResponseChannelClosed,
    #[error(transparent)]
    EncodingError(#[from] encoding::mock::Error),
    #[error("This peer is not assigned to shards from the operation")]
    NoShardsAssigned,
}

impl ShardProcessor {
    async fn execute(&self, program: Instructions) -> Vec<Result<Vid, Error>> {
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
                    warn!("did not execute operation: {}", e);
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

impl ShardProcessor {
    pub fn new(bus: MemoryBus) -> Self {
        Self { memory_access: bus }
    }

    pub async fn run(self, mut connection: ModuleChannelServer<Module>) {
        loop {
            tokio::select! {
                in_event = connection.input.recv() => {
                    let Some(in_event) = in_event else {
                        error!("`connection.output` is closed, shuttung down instruction memory");
                        return;
                    };
                    match in_event {
                        InEvent::Execute(program) => {
                            let (instructions, program_id) = program.into_parts();
                            let results = self.execute(instructions).await;
                            if let Err(_) = connection.output.send(
                                OutEvent::FinishedExecution { program_id, results }
                            ).await {
                                error!("`connection.output` is closed, shuttung down processor");
                                return;
                            }
                        },
                    }
                }
                _ = connection.shutdown.cancelled() => {
                    info!("received cancel signal, shutting down processor");
                    return;
                }
            }
        }
    }
}
