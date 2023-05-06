use std::collections::HashMap;

use libp2p::PeerId;
use tokio::sync::mpsc;

use crate::types::{Data, Shard, Sid, Vid};

use super::DataMemory;

pub struct Module;

impl crate::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type State = ();
}

pub enum OutEvent {
    // initial distribution
    /// Ready to answer to `ServeShard` and decided distribution of shards
    /// accross peers
    PreparedForService {
        data_id: Vid,
        distribution: Vec<(PeerId, Sid)>,
    },
    ServeShardResponse(super::FullShardId<DistributedDataMemory>, Option<Shard>),

    // assigned
    /// Successfully stored newly assigned shard
    AssignedStoreSuccess(super::FullShardId<DistributedDataMemory>),
    /// Give requested assigned shard
    AssignedShard {
        full_shard_id: super::FullShardId<DistributedDataMemory>,
        shard: Option<Shard>,
    },

    // data recollection
    /// Need the shard from peer `location`
    RequestAssigned {
        full_shard_id: super::FullShardId<DistributedDataMemory>,
        location: PeerId,
    },
    /// Successfully assembled data, ready to provide it to the user
    FinishedRecollection {
        data_id: Vid,
        data: Data,
    },
}

pub enum InEvent {
    // initial distribution
    // will store the location & set shard as successfully served if applicable
    TrackLocation {
        full_shard_id: super::FullShardId<DistributedDataMemory>,
        location: PeerId,
    },
    PrepareForService {
        data_id: Vid,
        data: Data,
    },
    ServeShardRequest(super::FullShardId<DistributedDataMemory>),

    // assigned
    StoreAssigned {
        full_shard_id: super::FullShardId<DistributedDataMemory>,
        shard: Shard,
    },
    GetAssigned(super::FullShardId<DistributedDataMemory>),

    // data recollection
    RecollectData(Vid),

    HandleRequested {
        full_shard_id: super::FullShardId<DistributedDataMemory>,
        shard: Shard,
    },
}

/// Async data memory/data manager. Intended to communicate
/// with behaviour through corresponding [`ModuleChannelServer`] (the
/// behaviour thus uses [`ModuleChannelClient`]).
///
/// Tracks locations of data shards, stores shards assigned to this peer,
/// and manages initial distribution (serving) with rebuilding of data.
///
/// Use [`Self::run()`] to operate.
pub struct DistributedDataMemory {
    data_locations: HashMap<Vid, Vec<(Sid, PeerId)>>,
    local_storage: HashMap<Vid, (Sid, Vid)>,
    to_distribute: HashMap<Vid, HashMap<Sid, Shard>>,
    currently_assembled: HashMap<Vid, HashMap<Sid, Shard>>,
}

pub struct Settings {
    pub data_shards_total: u64,
    pub data_shards_sufficient: u64,
}

struct MemoryBus {
    data_requests: mpsc::Receiver<(Vid, mpsc::Sender<Shard>)>,
    data_writes: mpsc::Receiver<(Vid, Vec<Shard>)>,
    settings: Settings,
}

impl DistributedDataMemory {
    // TODO: proper error
    pub fn handle_received_shard(
        &mut self,
        data_id: Vid,
        shard_id: Sid,
        shard: Shard,
    ) -> Result<(), ()> {
        let shards = self.currently_assembled.get_mut(&data_id).ok_or(())?;
        if shards.contains_key(&shard_id) {
            return Err(());
        }
        shards.insert(shard_id, shard);
        // check if # is enough & respond to request.
        todo!()
    }

    pub async fn operate(&mut self) {
        // check & start handling request
        //
    }
}

impl DataMemory for DistributedDataMemory {
    type Error = ();
    type Shard = Shard;
    type DataId = Vid;
    type ShardId = Sid;

    fn get_shard(&self, full_shard_id: &super::FullShardId<Self>) -> Option<&Self::Shard> {
        todo!()
    }

    fn store_shard(
        &mut self,
        full_shard_id: super::FullShardId<Self>,
        data: Self::Shard,
    ) -> Result<Option<Self::Shard>, Self::Error> {
        todo!()
    }

    fn remove_shard(
        &mut self,
        full_shard_id: &super::FullShardId<Self>,
    ) -> Result<Option<Self::Shard>, Self::Error> {
        todo!()
    }

    fn observe_new_location(&mut self, full_shard_id: &super::FullShardId<Self>, location: PeerId) {
        todo!()
    }

    fn prepare_to_serve_shards(
        &mut self,
        data: Self::DataId,
        shards: HashMap<Self::ShardId, Self::Shard>,
    ) {
        todo!()
    }

    fn serve_shard(&self, full_shard_id: &super::FullShardId<Self>) -> Option<Self::Shard> {
        todo!()
    }
}

// impl Stream for DistributedDataMemory {
//     type Item = ;

//     fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
//         todo!()
//     }
// }
