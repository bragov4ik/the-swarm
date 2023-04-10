use std::fmt::Debug;

pub mod mock;

pub trait DataMemory {
    type Error: Debug;
    type Identifier;
    type Data;

    /// Get data with given identifier, if present
    fn get(&self, id: &Self::Identifier) -> Option<&Self::Data>;

    /// Put data in the storage, updating and returning old value,
    /// if there was any.
    fn put(
        &mut self,
        id: Self::Identifier,
        data: Self::Data,
    ) -> Result<Option<Self::Data>, Self::Error>;

    /// Remove item with identifier `id` from the storage and return the
    /// item (if there was any)
    fn remove(&mut self, id: &Self::Identifier) -> Result<Option<Self::Data>, Self::Error>;
}
