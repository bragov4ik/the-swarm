use std::{collections::HashMap, fmt::Debug, hash::Hash};
use void::Void;

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

pub struct MemoryStorage<K, V> {
    inner: HashMap<K, V>,
}

impl<K, V> MemoryStorage<K, V> {
    pub fn new() -> Self {
        MemoryStorage { inner: HashMap::new() }
    }
}

impl<K, V> DataMemory for MemoryStorage<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    type Error = Void;
    type Identifier = K;
    type Data = V;

    fn get(&self, id: &Self::Identifier) -> Option<&Self::Data> {
        self.inner.get(id)
    }

    fn put(
        &mut self,
        id: Self::Identifier,
        data: Self::Data,
    ) -> Result<Option<Self::Data>, Self::Error> {
        Ok(self.inner.insert(id, data))
    }

    fn remove(&mut self, id: &Self::Identifier) -> Result<Option<Self::Data>, Self::Error> {
        Ok(self.inner.remove(id))
    }
}
