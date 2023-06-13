use std::collections::{hash_map, HashMap, HashSet};

use libp2p::PeerId;
use tracing::{debug, error, info, warn};

use crate::module::ModuleChannelServer;
use crate::processor::{Instructions, Program, ProgramIdentifier};
use crate::types::Vid;

mod traits;

pub struct Module;

impl crate::module::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type SharedState = ();
}

#[derive(Debug, Clone)]
pub enum OutEvent {
    /// Next program for execution is available.
    NextProgram(Program),
    /// Enough peers completed the program; we can safely consider it completed.
    FinishedExecution(ProgramIdentifier),
    /// The peer completed program with this identifier and its shards for
    /// provided data ids are updated during the execution
    PeerShardsActualized {
        program_id: ProgramIdentifier,
        peer: PeerId,
        updated_data_ids: Vec<Vid>,
    },
}

#[derive(Debug, Clone)]
pub enum InEvent {
    /// New program has been finalized
    FinalizedProgram(Program),
    /// Track completion of programs (`k` found - success)
    ExecutedProgram {
        peer: PeerId,
        program_id: ProgramIdentifier,
    },
}

/// Async data memory/data manager. Intended to communicate
/// with behaviour through corresponding [`ModuleChannelServer`] (the
/// behaviour thus uses [`ModuleChannelClient`]).
///
/// Stores scheduled programs and tracks their execution
///
/// Use [`Self::run()`] to operate.
pub struct InstructionMemory {
    currently_executed: HashMap<ProgramIdentifier, ProgramMetadata>,
    accept_threshold: usize,
}

impl InstructionMemory {
    pub fn new(execution_confirmations_threshold: usize) -> Self {
        Self {
            currently_executed: HashMap::new(),
            accept_threshold: execution_confirmations_threshold,
        }
    }

    /// Returns `true` if this notification resulted in `Finished`
    /// execution state.
    fn notify_executed(&mut self, who: PeerId, program: ProgramIdentifier) -> bool {
        let entry = self.currently_executed.entry(program.clone());
        let hash_map::Entry::Occupied(mut metadata) = entry else {
            warn!(
                "Received notification on execution, but the program is unknown. \
                If this is a legitimate tx, the execution request would've been \
                received earlier and the program would be tracked. It means that \
                the notification is sent by mistake or it's malicious. Ignoring."
            );
            return false;
        };
        let metadata = metadata.get_mut();
        match &mut metadata.state {
            ExecutionState::Executing { peers_finished } => {
                peers_finished.insert(who);
                if peers_finished.len() >= self.accept_threshold {
                    debug!(
                        "Program {:?} is finished by {} peers. It is enough for us.",
                        program,
                        peers_finished.len()
                    );
                    metadata.state = ExecutionState::Finished;
                    true
                } else {
                    false
                }
            }
            // just ignore, we already decided it
            ExecutionState::Finished => false,
        }
    }

    fn notify_finalized(&mut self, program: ProgramIdentifier, instructions: &Instructions) {
        match self.currently_executed.entry(program) {
            hash_map::Entry::Occupied(_) => warn!(
                "Finalized program is already known \
                Realistically can be if someone submitted 2 exactly the same programs"
            ),
            hash_map::Entry::Vacant(vacant) => {
                vacant.insert(ProgramMetadata::new_started_executing(instructions));
            }
        }
    }
}

struct ProgramMetadata {
    state: ExecutionState,
    affected_data_ids: Vec<Vid>,
}

impl ProgramMetadata {
    fn compute_affected_data_ids(instructions: &Instructions) -> Vec<Vid> {
        let mut affected_ids = HashSet::new();
        for i in instructions {
            affected_ids.insert(i.result.clone());
        }
        affected_ids.into_iter().collect()
    }

    fn new_started_executing(instrucitons: &Instructions) -> Self {
        let affected_data_ids = Self::compute_affected_data_ids(instrucitons);
        Self {
            state: Default::default(),
            affected_data_ids,
        }
    }
}

enum ExecutionState {
    Executing { peers_finished: HashSet<PeerId> },
    Finished,
}

impl Default for ExecutionState {
    fn default() -> Self {
        Self::Executing {
            peers_finished: Default::default(),
        }
    }
}

impl InstructionMemory {
    pub async fn run(mut self, mut connection: ModuleChannelServer<Module>) {
        loop {
            tokio::select! {
                in_event = connection.input.recv() => {
                    let Some(in_event) = in_event else {
                        error!("`connection.output` is closed, shuttung down instruction memory");
                        return;
                    };
                    match in_event {
                        InEvent::FinalizedProgram(program) => {
                            self.notify_finalized(program.identifier().clone(), program.instructions());
                            if (connection.output.send(
                                OutEvent::NextProgram(program)
                            ).await).is_err() {
                                error!("`connection.output` is closed, shuttung down instruction memory");
                                return;
                            }
                        },
                        InEvent::ExecutedProgram { peer, program_id } => {
                            if self.notify_executed(peer, program_id.clone()) && (connection.output.send(
                                    OutEvent::FinishedExecution(program_id.clone())
                                ).await).is_err() {
                                error!("`connection.output` is closed, shuttung down instruction memory");
                                return;
                            }
                            if let Some(metadata) = self.currently_executed.get(&program_id) {
                                let updated_data_ids = metadata.affected_data_ids.clone();
                                if (connection.output.send(
                                    OutEvent::PeerShardsActualized { program_id, peer, updated_data_ids }
                                ).await).is_err() {
                                    error!("`connection.output` is closed, shuttung down instruction memory");
                                    return;
                                }
                            }
                        },
                    }
                }
                _ = connection.shutdown.cancelled() => {
                    info!("received cancel signal, shutting down instruction memory");
                    return;
                }
            }
        }
    }
}
