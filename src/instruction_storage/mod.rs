use futures::Stream;
use libp2p::PeerId;

use crate::processor::Program;
use crate::types::Hash;

pub struct Module;

impl crate::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type State = ();
}

pub enum OutEvent {
    NextProgram(Program),
}

pub enum InEvent {
    FinalizedProgram(Program),
    /// Track completion of programs (`k` found - success)
    ExecutedProgram {
        peer: PeerId,
        program: Hash,
    },
}

pub trait InstructionMemory: Stream<Item = Self::Program> {
    type Error: std::fmt::Debug;
    type Program;

    /// Save program that is accepted by and observed in consensus
    fn receive_program(&mut self, program: Self::Program) -> Result<(), Self::Error>;
}

pub trait ProgramQueue<TProgram> {
    type Error: std::fmt::Debug;

    fn push(&mut self, program: TProgram) -> Result<(), Self::Error>;
}

pub trait InstructionBuffer<TProgram, TInstruction> {
    type Error: std::fmt::Debug;

    /// Add instruction to program buffer.
    fn push_instruction(&mut self, instruction: TInstruction) -> Result<(), Self::Error>;

    /// Create program from the buffer
    fn flush(self) -> TProgram;
}
