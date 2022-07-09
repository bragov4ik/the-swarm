pub trait InstructionMemory {
    type Error: std::fmt::Debug;
    type Instruction;

    /// Get next instruction from the graph according to determenistic ordering
    /// algorithm (this ensures that the state of vectors is the same in the network).
    ///
    /// In the future, might be a good idea to cache locations of shards instead
    /// of skipping "Stored" tx.
    ///
    /// Some(<instr>) if next one is ready, None if none left for now.
    fn next_instruction(&mut self) -> Option<Self::Instruction>;

    /// Add instruction to execution queue.
    fn push_instruction(&mut self, instruction: Self::Instruction) -> Result<(), Self::Error>;
}
