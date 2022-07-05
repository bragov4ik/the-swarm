use std::collections::VecDeque;

use super::InstructionMemory;


pub struct MockInstructionMemory<I> {
    inner: VecDeque<I>,
}

impl InstructionMemory for MockInstructionMemory<I> {
    type Error = Void;
    type Instruction = I;

    fn next_instruction(&mut self) -> Option<Self::Instruction> {
        self.inner.pop_back()
    }

    fn put_instruction(&mut self, instruction: Self::Instruction) -> Result<(), Self::Error> {
        self.inner.push_front(instruction);
        Ok(())
    }
}
