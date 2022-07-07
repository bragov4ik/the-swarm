
pub(crate) mod mock;

pub trait InstructionMemory {
    type Error: std::fmt::Debug;
    type Instruction;

    fn next_instruction(&mut self) -> Option<Self::Instruction>;
    fn push_instruction(&mut self, instruction: Self::Instruction) -> Result<(), Self::Error>;
}
