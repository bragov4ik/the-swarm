use futures::Stream;

use crate::processor::single_threaded::Program;

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
