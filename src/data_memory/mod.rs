//! Data storage and localization.
//!
//! This module is responsible for saving data assigned to this peer,
//! temporarily storing shards that are currently in the process
//! of distribution, and tracking location of other shards in the
//! network.
//!
//! Assigned data is handled via commonly-named functions [`DataMemory::get_shard()`],
//! [`DataMemory::store_shard()`], and [`DataMemory::remove_shard()`].
//!
//! The functions responsible for temporal data are [`DataMemory::prepare_to_serve_shards()`],
//! [`DataMemory::serve_shard()`], and (partly) [`DataMemory::observe_new_location()`].
//!
//! [`DataMemory::observe_new_location()`] mainly tracks locations of other data shards
//! but also helps to remove unnecessary stored temporal data.

use std::{collections::HashMap, fmt::Debug};

use libp2p::PeerId;

// pub mod mock;
pub mod distributed_simple;

pub type FullShardId<D> = (<D as DataMemory>::DataId, <D as DataMemory>::ShardId);

pub trait DataMemory {
    type Error: Debug;

    type Shard;
    type DataId;
    type ShardId;

    /// Get locally stored shard assigned to this peer, if present
    fn get_shard(&self, full_shard_id: &FullShardId<Self>) -> Option<&Self::Shard>;

    /// Put data assigned to this peer in the local storage, updating
    /// and returning old value, if there was any.
    fn store_shard(
        &mut self,
        full_shard_id: FullShardId<Self>,
        data: Self::Shard,
    ) -> Result<Option<Self::Shard>, Self::Error>;

    /// Remove shard from the local storage and return it (if there was any)
    fn remove_shard(
        &mut self,
        full_shard_id: &FullShardId<Self>,
    ) -> Result<Option<Self::Shard>, Self::Error>;

    /// Notify the data memory about observed location of some data shard.
    ///
    /// Data memeory is intended to track the shards (todo: maybe separate this
    /// responsibility into other module?); it's done via this method.
    ///
    /// Also this allows to track how many shards were already served and
    /// remove them if no longer needed.
    fn observe_new_location(&mut self, full_shard_id: &FullShardId<Self>, location: PeerId);

    /// Get ready to serve & track the progress of service of data shards during
    /// data distribution.
    ///
    /// The idea is to store them for some short time until they are all distributed.
    fn prepare_to_serve_shards(
        &mut self,
        data: Self::DataId,
        shards: HashMap<Self::ShardId, Self::Shard>,
    );

    /// Provide shard stored temporarily.
    fn serve_shard(&self, full_shard_id: &FullShardId<Self>) -> Option<Self::Shard>;
}
