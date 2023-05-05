use std::{collections::HashMap, hash::Hash};
use void::Void;

use crate::node::DataMemoryReadAll;

use super::DataMemory;

pub struct MemoryStorage<K, V> {
    inner: HashMap<K, V>,
}

impl<K, V> MemoryStorage<K, V> {
    pub fn new() -> Self {
        MemoryStorage {
            inner: HashMap::new(),
        }
    }
}

impl<K, V> DataMemory for MemoryStorage<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    type Error = Void;
    type Identifier = K;
    type Shard = V;

    fn get_shard(&self, id: &Self::Identifier) -> Option<&Self::Shard> {
        self.inner.get(id)
    }

    fn store_shard(
        &mut self,
        id: Self::Identifier,
        data: Self::Shard,
    ) -> Result<Option<Self::Shard>, Self::Error> {
        Ok(self.inner.insert(id, data))
    }

    fn remove_shard(&mut self, id: &Self::Identifier) -> Result<Option<Self::Shard>, Self::Error> {
        Ok(self.inner.remove(id))
    }
}

impl<K, V> DataMemoryReadAll<K, V> for MemoryStorage<K, V>
where
    K: Clone,
    V: Clone,
{
    fn read_all(&self) -> Vec<(K, V)> {
        self.inner
            .iter()
            .map(|(a, b)| (a.clone(), b.clone()))
            .collect()
    }
}
