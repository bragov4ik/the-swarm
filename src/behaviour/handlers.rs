use std::task::Poll;

use futures::{pin_mut, Future};
use libp2p::PeerId;
use rust_hashgraph::algorithm::event::Hash;
use tracing::{debug, error};

use crate::{
    channel_log_send,
    consensus::Transaction,
    data_memory, instruction_storage,
    logging_helpers::Targets,
    processor::{Instructions, Program, ProgramIdentifier},
    types::{Sid, Vid},
};

use super::Behaviour;

pub enum HandleResult {
    Ok,
    Abort,
}

impl Behaviour {
    fn handle_storage_request_tx(
        &mut self,
        cx: &mut std::task::Context<'_>,
        data_id: Vid,
        from: PeerId,
    ) -> HandleResult {
        debug!(
            target: Targets::DataDistribution.into_str(),
            "Recognized finalized storage request for {:?}", data_id
        );
        let event = data_memory::InEvent::StorageRequestTx(data_id, from);
        let send_future = self.data_memory.input.send(event.clone());
        pin_mut!(send_future);
        match send_future.poll(cx) {
            Poll::Ready(Ok(_)) => {
                channel_log_send!("data_memory.input", format!("{:?}", event));
                HandleResult::Ok
            }
            Poll::Ready(Err(_e)) => {
                error!("other half of `data_memory.input` was closed. cannot operate without this module.");
                HandleResult::Abort
            }
            Poll::Pending => {
                error!("`data_memory.input` queue is full. continuing will lose track of stored shards.");
                HandleResult::Abort
            }
        }
    }

    fn handle_stored_ts(
        &mut self,
        cx: &mut std::task::Context<'_>,
        data_id: Vid,
        shard_id: Sid,
        from: PeerId,
    ) -> HandleResult {
        debug!(
            target: Targets::DataDistribution.into_str(),
            "Recognized finalized storage confirmation for {:?} by {:?}", data_id, from
        );
        let event = data_memory::InEvent::StoreConfirmed {
            full_shard_id: (data_id, shard_id),
            location: from,
        };
        let send_future = self.data_memory.input.send(event.clone());
        pin_mut!(send_future);
        match send_future.poll(cx) {
            Poll::Ready(Ok(_)) => {
                channel_log_send!("data_memory.input", format!("{:?}", event));
                HandleResult::Ok
            }
            Poll::Ready(Err(_e)) => {
                error!("other half of `data_memory.input` was closed. cannot operate without this module.");
                HandleResult::Abort
            }
            Poll::Pending => {
                error!("`data_memory.input` queue is full. continuing will lose track of stored shards.");
                HandleResult::Abort
            }
        }
    }

    fn handle_execute_tx(
        &mut self,
        cx: &mut std::task::Context<'_>,
        event_hash: Hash,
        instructions: Instructions,
    ) -> HandleResult {
        let program = match Program::new(instructions, event_hash.into()) {
            Ok(p) => p,
            Err(e) => {
                error!("could not compute hash of a program: {}", e);
                return HandleResult::Abort;
            }
        };
        let identifier = program.identifier().clone();
        let send_future = self
            .instruction_memory
            .input
            .send(instruction_storage::InEvent::FinalizedProgram(program));
        pin_mut!(send_future);
        match send_future.poll(cx) {
            Poll::Ready(Ok(_)) => {
                channel_log_send!(
                    "instruction_memory.input",
                    format!("FinalizedProgram(hash: {:?})", identifier)
                );
                HandleResult::Ok
            }
            Poll::Ready(Err(_e)) => {
                error!(
                    "other half of `instruction_memory.input` was closed. \
                        cannot operate without this module."
                );
                HandleResult::Abort
            }
            Poll::Pending => {
                error!(
                    "`instruction_memory.input` queue is full. \
                    continue will skip a transaction, which is unacceptable."
                );
                HandleResult::Abort
            }
        }
    }

    fn handle_executed_tx(
        &mut self,
        cx: &mut std::task::Context<'_>,
        from: PeerId,
        program_id: ProgramIdentifier,
    ) -> HandleResult {
        let event = instruction_storage::InEvent::ExecutedProgram {
            peer: from,
            program_id,
        };
        let send_future = self.instruction_memory.input.send(event.clone());
        pin_mut!(send_future);
        match send_future.poll(cx) {
            Poll::Ready(Ok(_)) => {
                channel_log_send!("instruction_memory.input", format!("{:?}", event));
                HandleResult::Ok
            }
            Poll::Ready(Err(_e)) => {
                error!(
                    "other half of `instruction_memory.input` was closed. \
                        cannot operate without this module."
                );
                HandleResult::Abort
            }
            Poll::Pending => {
                error!(
                    "`instruction_memory.input` queue is full. continue will \
                    mess with confirmation of program execution, which is \
                    unacceptable."
                );
                HandleResult::Abort
            }
        }
    }

    fn handle_initialize_storage_tx(
        &mut self,
        cx: &mut std::task::Context<'_>,
        distribution: Vec<(PeerId, Sid)>,
    ) -> HandleResult {
        debug!(
            target: Targets::StorageInitialization.into_str(),
            "Recognized finalized init transaction"
        );
        let send_future = self
            .data_memory
            .input
            .send(data_memory::InEvent::Initialize { distribution });
        pin_mut!(send_future);
        match send_future.poll(cx) {
            Poll::Ready(Ok(_)) => {
                channel_log_send!("data_memory.input", "Initialize");
                HandleResult::Ok
            }
            Poll::Ready(Err(_e)) => {
                error!(
                    "other half of `data_memory.input` was closed. \
                        cannot operate without this module."
                );
                HandleResult::Abort
            }
            Poll::Pending => {
                error!(
                    "`data_memory.input` queue is full. continuing will \
                    lose track of stored shards."
                );
                HandleResult::Abort
            }
        }
    }

    pub(super) fn handle_tx(
        &mut self,
        cx: &mut std::task::Context<'_>,
        from: PeerId,
        tx: Transaction<Vid, Sid, PeerId>,
        event_hash: Hash,
    ) -> HandleResult {
        match tx {
            // track data locations, pull assigned shards
            Transaction::StorageRequest { data_id } => {
                self.handle_storage_request_tx(cx, data_id, from)
            }
            // take a note that `(data_id, shard_id)` is stored at `location`
            Transaction::Stored(data_id, shard_id) => {
                self.handle_stored_ts(cx, data_id, shard_id, from)
            }
            Transaction::Execute(instructions) => {
                self.handle_execute_tx(cx, event_hash, instructions)
            }
            Transaction::Executed(program_id) => self.handle_executed_tx(cx, from, program_id),
            Transaction::InitializeStorage { distribution } => {
                self.handle_initialize_storage_tx(cx, distribution)
            }
        }
    }
}
