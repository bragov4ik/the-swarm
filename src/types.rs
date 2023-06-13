use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

use crate::consensus::graph::SyncJobs;

/// Identifier for whole data unit (not split into shards). For example,
/// this can be a memory address. Shards of the vector will have the same `Vid`.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Hash)]
pub struct Vid(pub u64);

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Hash)]
pub struct Sid(pub u64);

#[cfg(all(feature = "big-array", feature = "medium-array"))]
compile_error!("'big-array' and 'medium-array' features are mutually exclusive");

#[cfg(feature = "big-array")]
pub const SHARD_BYTES_NUMBER: u64 = 2u64.pow(14);
#[cfg(feature = "medium-array")]
pub const SHARD_BYTES_NUMBER: u64 = 64;
#[cfg(all(not(feature = "big-array"), not(feature = "medium-array")))]
pub const SHARD_BYTES_NUMBER: u64 = 4;
// parity shards are configured dynamically
pub const DATA_SHARDS_COUNT: u64 = 2;

/// Type/struct that represents unit of data stored on nodes.
/// Should be actual data shard (erasure coded) in the future, but
/// right now for demonstration purposes, represents vector(array) of size 4.
#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct Shard(#[serde(with = "BigArray")] pub [u8; SHARD_BYTES_NUMBER as usize]);

impl Shard {
    pub fn as_inner(&self) -> &[u8; SHARD_BYTES_NUMBER as usize] {
        &self.0
    }
}

impl AsRef<[u8]> for Shard {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsMut<[u8]> for Shard {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

/// for now just splits into three parts
#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct Data(
    #[serde(with = "BigArray")] pub [u8; (SHARD_BYTES_NUMBER * DATA_SHARDS_COUNT) as usize],
);

impl Data {
    pub fn as_inner(&self) -> &[u8; (SHARD_BYTES_NUMBER * DATA_SHARDS_COUNT) as usize] {
        &self.0
    }
}

impl AsRef<[u8]> for Data {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsMut<[u8]> for Data {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

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
        self.inner
    }

    pub fn as_ref(&self) -> &[u8; 64] {
        &self.inner
    }

    pub const fn from_array(inner: [u8; 64]) -> Self {
        Hash { inner }
    }
}

impl From<rust_hashgraph::algorithm::event::Hash> for Hash {
    fn from(value: rust_hashgraph::algorithm::event::Hash) -> Self {
        Self::from_array(value.into_array())
    }
}

impl From<Hash> for rust_hashgraph::algorithm::event::Hash {
    fn from(val: Hash) -> Self {
        rust_hashgraph::algorithm::event::Hash::from_array(val.into_array())
    }
}
