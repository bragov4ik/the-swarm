use void::Void;
use std::{collections::HashMap, hash::Hash};

pub trait DataMemory {
    type Error;
    type Identifier;
    type Data;

    fn get(&self, id: &Self::Identifier) -> Option<Self::Data>;
    fn put(&mut self, id: Self::Identifier, data: Self::Data) -> Result<Option<Self::Data>, Self::Error>;
    fn remove(&mut self, id: &Self::Identifier) -> Result<Option<Self::Data>, Self::Error>;
}

pub struct MemoryStorage<K, V> {
    inner: HashMap<K, V>
}

impl<K, V> DataMemory for MemoryStorage<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    type Error = Void;
    type Identifier = K;
    type Data = V;

    fn get(&self, id: &Self::Identifier) -> Option<Self::Data> {
        self.inner.get(id).cloned()
    }

    fn put(&mut self, id: Self::Identifier, data: Self::Data) -> Result<Option<Self::Data>, Self::Error> {
        Ok(self.inner.insert(id, data))
    }

    fn remove(&mut self, id: &Self::Identifier) -> Result<Option<Self::Data>, Self::Error> {
        Ok(self.inner.remove(id))
    }
}