use std::collections::{hash_map, HashMap, HashSet};

use libp2p::PeerId;
use tracing::{debug, error, warn};

use crate::module::ModuleChannelServer;
use crate::processor::{Program, ProgramIdentifier};

mod traits;

pub struct Module;

impl crate::module::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type SharedState = ();
}

pub enum OutEvent {
    /// Next program for execution is available.
    NextProgram(Program),
    /// Enough peers completed the program; we can safely consider it completed.
    FinishedExecution(ProgramIdentifier),
}

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
    currently_executed: HashMap<ProgramIdentifier, ExecutionState>,
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
        if let hash_map::Entry::Vacant(_) = &entry {
            // todo: might be abused to create more and more HashSets.
            // this is not currently considered a misbehaviour by the network,
            // so this needs to be checked out.
            warn!(
                "Received notification on execution, but the program is unknown. \
            Either someone is blazingly fast or the program is not (and will never be) \
            known to the peers at all. Tracking just in case of blazing speed."
            );
        }
        let state = entry.or_default();
        match state {
            ExecutionState::Executing { peers_finished } => {
                peers_finished.insert(who);
                if peers_finished.len() >= self.accept_threshold {
                    debug!(
                        "Program {:?} is finished by {} peers. It is enough for us.",
                        program,
                        peers_finished.len()
                    );
                    *state = ExecutionState::Finished;
                    true
                } else {
                    false
                }
            }
            // just ignore, we already decided it
            ExecutionState::Finished => false,
        }
    }

    fn notify_finalized(&mut self, program: ProgramIdentifier) {
        match self.currently_executed.entry(program) {
            hash_map::Entry::Occupied(_) => warn!(
                "Finalized program is already known \
                Realistically can be if someone submitted 2 exactly the same programs or\
                in case 'blazing fast' peer, see `notify_executed`"
            ),
            hash_map::Entry::Vacant(vacant) => {
                vacant.insert(ExecutionState::default());
            }
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
                            self.notify_finalized(program.identifier().clone());
                            if let Err(_) = connection.output.send(
                                OutEvent::NextProgram(program)
                            ).await {
                                error!("`connection.output` is closed, shuttung down instruction memory");
                                return;
                            }
                        },
                        InEvent::ExecutedProgram { peer, program_id } => {
                            if self.notify_executed(peer, program_id.clone()) {
                                if let Err(_) = connection.output.send(
                                    OutEvent::FinishedExecution(program_id)
                                ).await {
                                    error!("`connection.output` is closed, shuttung down instruction memory");
                                    return;
                                }
                            }
                        },
                    }
                }
            }
        }
    }
}
