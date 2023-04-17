use std::collections::VecDeque;

use void::Void;

use super::InstructionMemory;


pub struct MockInstructionMemory<I> {
    inner: VecDeque<I>,
}

impl<I> InstructionMemory for MockInstructionMemory<I> {
    type Error = Void;
    type Instruction = I;

    fn next_instruction(&mut self) -> Option<Self::Instruction> {
        self.inner.pop_back()
    }

    fn push_instruction(&mut self, instruction: Self::Instruction) -> Result<(), Self::Error> {
        self.inner.push_front(instruction);
        Ok(())
    }
}
