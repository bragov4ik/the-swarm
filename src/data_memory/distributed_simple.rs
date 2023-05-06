use std::collections::{hash_map, HashMap};

use futures::channel::oneshot;
use libp2p::PeerId;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use crate::{
    behaviour::ModuleChannelServer,
    encoding::{mock::MockEncoding, DataEncoding},
    types::{Data, Shard, Sid, Vid},
};

use super::DataMemory;

pub struct Module;

impl crate::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type State = ();
}

pub type FullShardId = (Vid, Sid);

pub enum OutEvent {
    // initial distribution
    /// Ready to answer to `ServeShard` and decided distribution of shards
    /// accross peers
    PreparedForService {
        data_id: Vid,
        distribution: Vec<(PeerId, Sid)>,
    },
    /// The shard that was requested to store here
    ServeShardResponse(FullShardId, Option<Shard>),

    // assigned
    /// Successfully stored newly assigned shard
    AssignedStoreSuccess(FullShardId),
    /// Give requested assigned shard
    AssignedShard {
        full_shard_id: FullShardId,
        shard: Option<Shard>,
    },

    // data recollection
    /// Need the shard from peer `location`
    RequestAssigned {
        full_shard_id: FullShardId,
        location: PeerId,
    },
    /// Successfully assembled data, ready to provide it to the user
    FinishedRecollection { data_id: Vid, data: Data },
}

pub enum InEvent {
    // initial distribution
    // will store the location & set shard as successfully served if applicable
    TrackLocation {
        full_shard_id: FullShardId,
        location: PeerId,
    },
    PrepareForService {
        data_id: Vid,
        data: Data,
    },
    /// The shard was requested for storage by another node
    ServeShardRequest(FullShardId),

    // assigned
    /// Store shard assigned to this node
    StoreAssigned {
        full_shard_id: FullShardId,
        shard: Shard,
    },
    /// Give shard assigned to the node
    GetAssigned(FullShardId),

    // data recollection
    /// Recollect data with given id, request by user
    RecollectData(Vid),

    /// Handle shard that was previously requested by us
    HandleRequested {
        full_shard_id: FullShardId,
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
    data_locations: HashMap<Vid, HashMap<Sid, PeerId>>,
    local_storage: HashMap<Vid, HashMap<Sid, Shard>>,
    to_distribute: HashMap<Vid, HashMap<Sid, Shard>>,
    currently_assembled: HashMap<Vid, HashMap<Sid, Shard>>,
    awaiting_assembles: HashMap<Vid, AwaitingAssembly>,
    bus: MemoryBus,
    encoding: MockEncoding,
}

enum AwaitingAssembly {
    ModuleClient,
    MemoryBus(Vec<oneshot::Sender<Data>>),
    Both(Vec<oneshot::Sender<Data>>),
}

impl AwaitingAssembly {
    fn add_memory_bus_handle(&mut self, handle: oneshot::Sender<Data>) {
        match self {
            AwaitingAssembly::ModuleClient => *self = AwaitingAssembly::Both(vec![handle]),
            AwaitingAssembly::MemoryBus(list) => list.push(handle),
            AwaitingAssembly::Both(list) => list.push(handle),
        }
    }
    fn add_module_client(&mut self) {
        match self {
            AwaitingAssembly::MemoryBus(list) => {
                // change enum variant to `Both`
                let mut list_2 = vec![];
                std::mem::swap(list, &mut list_2);
                *self = AwaitingAssembly::Both(list_2)
            }
            AwaitingAssembly::Both(_) | AwaitingAssembly::ModuleClient => (),
        }
    }
}

pub enum MemoryBusDataRequest {
    /// Gather shards, rebuild, and provide full data
    Assemble {
        data_id: Vid,
        response_handle: oneshot::Sender<Data>,
    },
    /// Only pass locally stored shard(-s?)
    AssignedShards {
        data_id: Vid,
        response_handle: oneshot::Sender<Vec<Shard>>,
    },
}

struct MemoryBus {
    data_requests: mpsc::Receiver<MemoryBusDataRequest>,
    /// Write results of calculation to assigned shards
    data_writes: mpsc::Receiver<(Vid, Vec<Shard>)>,
}

impl DistributedDataMemory {
    /// Get locally stored shard assigned to this peer, if present
    fn get_shard(&self, full_shard_id: &FullShardId) -> Option<&Shard> {
        let shards = self.local_storage.get(&full_shard_id.0)?;
        shards.get(&full_shard_id.1)
    }

    /// Put data assigned to this peer in the local storage, updating
    /// and returning old value, if there was any.
    fn store_shard(&mut self, full_shard_id: FullShardId, data: Shard) -> Option<Shard> {
        let shards = self.local_storage.entry(full_shard_id.0).or_default();
        shards.insert(full_shard_id.1, data)
    }

    /// Remove shard from the local storage and return it (if there was any)
    fn remove_shard(&mut self, full_shard_id: &FullShardId) -> Option<Shard> {
        self.local_storage
            .get_mut(&full_shard_id.0)
            .map(|shards| shards.remove(&full_shard_id.1))
            .flatten()
    }

    /// Notify the data memory about observed location of some data shard.
    ///
    /// Data memeory is intended to track the shards (todo: maybe separate this
    /// responsibility into other module?); it's done via this method.
    ///
    /// Also this allows to track how many shards were already served and
    /// remove them if no longer needed.
    fn observe_new_location(&mut self, full_shard_id: &FullShardId, location: PeerId) {
        let shards = self.data_locations.entry(full_shard_id.0).or_default();
        shards.insert(full_shard_id.1, location);
    }

    /// Get ready to serve & track the progress of service of data shards during
    /// data distribution.
    ///
    /// The idea is to store them for some short time until they are all distributed.
    fn prepare_to_serve_shards(&mut self, data_id: Vid, shards: HashMap<Sid, Shard>) {
        if let Some(previously_distributed) = self.to_distribute.insert(data_id, shards) {
            warn!(
                "Started distribution of vector that has not finished distributing yet \
            ({} pieces were not confirmed to be stored). \
            Likely someone didn't get their last piece and we just reuse the address {:?}",
                previously_distributed.len(),
                data_id
            );
        }
    }

    /// Provide shard stored temporarily.
    fn serve_shard(&self, full_shard_id: &FullShardId) -> Option<Shard> {
        let shards = self.to_distribute.get(&full_shard_id.0)?;
        shards.get(&full_shard_id.1).cloned()
    }

    // TODO: proper error
    ///
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
}

impl DistributedDataMemory {
    async fn run(mut self, mut connection: ModuleChannelServer<Module>) {
        loop {
            tokio::select! {
                in_event = connection.input.recv() => {
                    let Some(in_event) = in_event else {
                        error!("`connection.output` is closed, shuttung down data memory");
                        return;
                    };
                    if let Err(_) = connection.output.send(OutEvent::NextProgram(program)).await {
                        error!("`connection.output` is closed, shuttung down data memory");
                        return;
                    }
                    match in_event {
                        InEvent::TrackLocation { full_shard_id, location } => todo!(),
                        InEvent::PrepareForService { data_id, data } => todo!(),
                        InEvent::ServeShardRequest(_) => todo!(),
                        InEvent::StoreAssigned { full_shard_id, shard } => todo!(),
                        InEvent::GetAssigned(_) => todo!(),
                        InEvent::RecollectData(data_id) => {
                            match self.currently_assembled.entry(data_id) {
                                // requests were already sent, just join the waiting list
                                hash_map::Entry::Occupied(_) => (),
                                hash_map::Entry::Vacant(v) => {
                                    // yes, code duplication. what are you gonna do? 🤨
                                    let Some(locations) = self.data_locations.get(&data_id) else {
                                        warn!("user requested unknown data, ignoring");
                                        continue;
                                    };
                                    for (shard, owner) in locations {
                                        if let Err(_) = connection.output.send(OutEvent::RequestAssigned{
                                            full_shard_id: (data_id, shard.clone()),
                                            location: owner.clone()
                                        }).await {
                                            error!("`connection.output` is closed, shuttung down data memory");
                                            return;
                                        }
                                    }
                                    v.insert(HashMap::new());
                                },
                            }
                            let awaiting_assembles = self.awaiting_assembles.entry(data_id)
                                .or_insert(AwaitingAssembly::ModuleClient);
                            awaiting_assembles.add_module_client();
                        },
                        InEvent::HandleRequested { full_shard_id, shard } => {
                            if let Some(received_shards) = self.currently_assembled.get_mut(&full_shard_id.0) {
                                if !received_shards.contains_key(&full_shard_id.1) {
                                    received_shards.insert(full_shard_id.1, shard);
                                    if received_shards.len() >= self.encoding.settings().data_shards_sufficient.try_into().expect("# of shards sufficient is too large") {

                                    }
                                }
                                else {
                                    // not sure when it's possible
                                    warn!("received shard that is already present, weird");
                                }
                            }
                            else {
                                debug!("received shard was likely already assembled, skipping");
                            }
                        },
                    }
                }
                data_request = self.bus.data_requests.recv() => {
                    let Some(data_request) = data_request else {
                        error!("memory bus is closed, shuttung down data memory");
                        return;
                    };
                    match data_request {
                        MemoryBusDataRequest::Assemble { data_id, response_handle } => {
                            match self.currently_assembled.entry(data_id) {
                                // requests were already sent, just join the waiting list
                                hash_map::Entry::Occupied(_) => (),
                                hash_map::Entry::Vacant(v) => {
                                    // yes, code duplication. what are you gonna do? 🤨
                                    let Some(locations) = self.data_locations.get(&data_id) else {
                                        warn!("memory bus requested unknown data, ignoring");
                                        continue;
                                    };
                                    for (shard, owner) in locations {
                                        if let Err(_) = connection.output.send(OutEvent::RequestAssigned{
                                            full_shard_id: (data_id, shard.clone()),
                                            location: owner.clone()
                                        }).await {
                                            error!("`connection.output` is closed, shuttung down data memory");
                                            return;
                                        }
                                    }
                                    v.insert(HashMap::new());
                                },
                            }
                            let awaiting_assembles = self.awaiting_assembles.entry(data_id)
                                .or_insert(AwaitingAssembly::MemoryBus(vec![]));
                            awaiting_assembles.add_memory_bus_handle(response_handle);
                        },
                        MemoryBusDataRequest::AssignedShards { data_id, response_handle } => {
                            let shards: Vec<_> = self.local_storage.get(&data_id)
                                .map(|mapping| mapping
                                    .values()
                                    .into_iter()
                                    .cloned()
                                    .collect())
                                .unwrap_or_default();
                            if let Err(e) = response_handle.send(shards) {
                                warn!("response handle for memory bus request is closed, shouldn't happen \
                                    but let's try to continue operation");
                            }
                        },
                    }
                }
            }
        }
    }
}
