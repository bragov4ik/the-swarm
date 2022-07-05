
pub trait InstructionMemory {
    type Error;
    type Instruction;

    fn next_instruction(&mut self) -> Option<Self::Instruction>;
    fn put_instruction(&mut self, instruction: Self::Instruction) -> Result<(), Self::Error>;
}
