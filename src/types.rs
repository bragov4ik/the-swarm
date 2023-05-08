use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

use crate::consensus::graph::SyncJobs;

/// Identifier for whole data unit (not split into shards). For example,
/// this can be a memory address. Shards of the vector will have the same `Vid`.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Hash)]
pub struct Vid(pub u64);

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Hash)]
pub struct Sid(pub u64);

pub const SHARD_BYTES_NUMBER: u64 = 4;
// parity shards are configured dynamically
pub const DATA_SHARDS_COUNT: u64 = 3;

/// Type/struct that represents unit of data stored on nodes.
/// Should be actual data shard (erasure coded) in the future, but
/// right now for demonstration purposes, represents vector(array) of size 4.
// #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub type Shard = [u8; SHARD_BYTES_NUMBER as usize];

/// for now just splits into three parts
pub type Data = [u8; (SHARD_BYTES_NUMBER * DATA_SHARDS_COUNT) as usize];

/// Graph representation that is passed on random gossip.
pub type GraphSync = SyncJobs<Vid, Sid>;

// smth like H256 ??? (some hash type)
#[derive(Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, Clone)]
pub struct Hash {
    #[serde(with = "BigArray")]
    inner: [u8; 64],
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:X?}", self.inner)
    }
}

impl std::fmt::Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hash")
            .field("hex_value", &format!("{self}"))
            .finish()
    }
}

impl std::ops::BitXor for &Hash {
    type Output = Hash;

    fn bitxor(self, rhs: Self) -> Self::Output {
        let mut result = [0u8; 64];
        for (i, (b1, b2)) in self.inner.iter().zip(rhs.inner.iter()).enumerate() {
            result[i] = b1 ^ b2;
        }
        Hash::from_array(result)
    }
}

impl std::ops::BitXor<&Hash> for Hash {
    type Output = Hash;

    fn bitxor(mut self, rhs: &Self) -> Self::Output {
        for i in 0..self.inner.len() {
            self.inner[i] ^= rhs.inner[i];
        }
        self
    }
}

impl Hash {
    pub fn into_array(self) -> [u8; 64] {
        return self.inner;
    }

    pub fn as_ref(&self) -> &[u8; 64] {
        return &self.inner;
    }

    pub const fn from_array(inner: [u8; 64]) -> Self {
        return Hash { inner };
    }
}
