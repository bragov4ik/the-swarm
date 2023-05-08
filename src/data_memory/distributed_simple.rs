use std::collections::{hash_map, HashMap, HashSet};

use futures::channel::oneshot;
use libp2p::PeerId;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::{
    behaviour::ModuleChannelServer,
    encoding::{
        reed_solomon::{self, ReedSolomonWrapper},
        DataEncoding,
    },
    types::{Data, Shard, Sid, Vid},
};

pub struct Module;

impl crate::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type SharedState = ();
}

pub type FullShardId = (Vid, Sid);

pub enum OutEvent {
    // initial distribution
    /// Ready to answer to `ServeShard` and decided distribution of shards
    /// accross peers
    PreparedForService { data_id: Vid },
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

#[derive(Debug)]
pub enum InEvent {
    Initialize {
        distribution: Vec<(PeerId, Sid)>,
    },

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

    // assigned data
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

pub struct UninitializedDataMemory {
    bus: MemoryBus,
    encoding: ReedSolomonWrapper,
}

impl UninitializedDataMemory {
    fn new(bus: MemoryBus, encoding_settings: reed_solomon::Settings) -> Self {
        let encoding = ReedSolomonWrapper::new(encoding_settings);
        Self { bus, encoding }
    }

    /// `true` == valid
    fn verify_distribution(&self, distribution: &HashMap<PeerId, Sid>) -> bool {
        let shard_ids: HashSet<_> = distribution.values().collect();
        let expected_ids: HashSet<_> = (0..)
            .take(
                self.encoding
                    .settings()
                    .data_shards_total
                    .try_into()
                    .unwrap(),
            )
            .map(|i| &Sid(i))
            .collect();
        shard_ids == expected_ids
    }

    fn initialize(self, distribution: HashMap<PeerId, Sid>) -> InitializedDataMemory {
        InitializedDataMemory {
            local_storage: HashMap::new(),
            to_distribute: HashMap::new(),
            currently_assembled: HashMap::new(),
            distribution,
            bus: self.bus,
            encoding: self.encoding,
        }
    }

    async fn run(
        mut self,
        connection: &mut ModuleChannelServer<Module>,
    ) -> Option<InitializedDataMemory> {
        loop {
            // todo: format the code semi-automatically (for each `select!`)
            tokio::select! {
                in_event = connection.input.recv() => {
                    let Some(in_event) = in_event else {
                        error!("`connection.output` is closed, shuttung down data memory");
                        return None;
                    };
                    match in_event {
                        InEvent::Initialize { distribution } => {
                            let distribution = distribution.into_iter().collect();
                            if !self.verify_distribution(&distribution) {
                                warn!("received distribution doesn't match expected pattern; \
                                expected to have a peer for each shard id from 0 to <total shard number>");
                                continue;
                            }
                            info!("storage initialized, ready");
                            return Some(self.initialize(distribution))
                        },
                        InEvent::TrackLocation {
                            full_shard_id: _,
                            location: _,
                        }
                        | InEvent::PrepareForService {
                            data_id: _,
                            data: _,
                        }
                        | InEvent::ServeShardRequest(_)
                        | InEvent::StoreAssigned {
                            full_shard_id: _,
                            shard: _,
                        }
                        | InEvent::GetAssigned(_)
                        | InEvent::RecollectData(_)
                        | InEvent::HandleRequested {
                            full_shard_id: _,
                            shard: _,
                        } => warn!("have not initialize storage, ignoring request {:?}", in_event),
                    }

                }
                data_request = self.bus.shard_requests.recv() => {
                    let Some(data_request) = data_request else {
                        error!("memory bus is closed, shuttung down data memory");
                        return None;
                    };
                    warn!("have not initialize storage, ignoring request from memory bus");
                }
            }
        }
    }
}

/// Async data memory/data manager. Intended to communicate
/// with behaviour through corresponding [`ModuleChannelServer`] (the
/// behaviour thus uses [`ModuleChannelClient`]).
///
/// Tracks locations of data shards, stores shards assigned to this peer,
/// and manages initial distribution (serving) with rebuilding of data.
///
/// Use [`Self::run()`] to operate.
struct InitializedDataMemory {
    // `HashMap<Sid, Shard>` because in the future we might store multiple shards on each peer
    local_storage: HashMap<Vid, HashMap<Sid, Shard>>,
    to_distribute: HashMap<Vid, HashMap<Sid, Shard>>,
    currently_assembled: HashMap<Vid, HashMap<Sid, Shard>>,
    /// `None` means it is the memory (and the system) is not active
    distribution: HashMap<PeerId, Sid>,
    bus: MemoryBus,
    encoding: ReedSolomonWrapper,
}

pub enum MemoryBusDataRequest {
    /// Gather shards, rebuild, and provide full data
    Assemble {
        data_id: Vid,
        response_handle: oneshot::Sender<Data>,
    },
    /// Only pass locally stored shard(-s?)
    AssignedShard {
        data_id: Vid,
        response_handle: oneshot::Sender<Shard>,
    },
}

struct MemoryBus {
    shard_requests: mpsc::Receiver<(Vid, oneshot::Sender<Option<Shard>>)>,
    /// Write results of calculation to assigned shards
    shard_writes: mpsc::Receiver<(Vid, Shard)>,
}

impl InitializedDataMemory {
    pub fn new(uninitialized: UninitializedDataMemory, distribution: HashMap<PeerId, Sid>) -> Self {
        uninitialized.initialize(distribution)
    }

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
    /// Data memeory may track the shards if the distribution becomes
    /// non-uniform in the future.
    ///
    /// Also this allows to track how many shards were already served and
    /// remove them if no longer needed.
    fn observe_new_location(&mut self, full_shard_id: FullShardId, location: PeerId) {
        if let Some(distributed_shards) = self.to_distribute.get_mut(&full_shard_id.0) {
            distributed_shards.remove(&full_shard_id.1);
        }
        if let Some(expected_location) = self.distribution.get(&location) {
            if expected_location != &full_shard_id.1 {
                warn!(
                    "observed shard at unexpected location. observed at {:?}, expected at {:?}",
                    &full_shard_id.1, expected_location
                );
            }
        } else {
            warn!("observed location does not appear in known data distribution. shouldn't happen");
        }

        // track location

        // let shards = self.data_locations.entry(full_shard_id.0).or_default();
        // shards.insert(full_shard_id.1, location);
    }

    /// Get ready to serve & track the progress of service of data shards during
    /// data distribution.
    ///
    /// The idea is to store them for some short time until they are all distributed.
    fn prepare_to_serve_shards(&mut self, data_id: Vid, shards: HashMap<Sid, Shard>) {
        if let Some(previously_distributed) = self.to_distribute.insert(data_id.clone(), shards) {
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
}

impl InitializedDataMemory {
    async fn run(mut self, connection: &mut ModuleChannelServer<Module>) {
        loop {
            // todo: format the code semi-automatically (for each `select!`)
            tokio::select! {
                in_event = connection.input.recv() => {
                    let Some(in_event) = in_event else {
                        error!("`connection.output` is closed, shuttung down data memory");
                        return;
                    };
                    // if let Err(_) = connection.output.send(OutEvent::NextProgram(program)).await {
                    //     error!("`connection.output` is closed, shuttung down data memory");
                    //     return;
                    // }
                    match in_event {
                        InEvent::Initialize { distribution } => {
                            warn!("received `InitializeStorage` transaction but storage was already initialized. ignoring");
                        },
                        // initial distribution
                        InEvent::TrackLocation { full_shard_id, location } => self.observe_new_location(full_shard_id, location),
                        InEvent::PrepareForService { data_id, data } => {
                            let shards = self.encoding.encode(data).expect(
                                "Encoding settings are likely incorrect, \
                                all problems should've been caught at type-level"
                            );
                            if let Some(_) = self.to_distribute.insert(data_id.clone(), shards) {
                                warn!("data was already serviced at id {:?}. discarding previous distribution.", data_id);
                            }
                            if let Err(_) = connection.output.send(
                                OutEvent::PreparedForService { data_id }
                            ).await {
                                error!("`connection.output` is closed, shuttung down data memory");
                                return;
                            }
                        },
                        InEvent::ServeShardRequest(_) => todo!(),
                        // assigned data
                        InEvent::StoreAssigned { full_shard_id, shard } => {
                            self.store_shard(full_shard_id.clone(), shard);
                            if let Err(_) = connection.output.send(OutEvent::AssignedStoreSuccess(full_shard_id)).await {
                                error!("`connection.output` is closed, shuttung down data memory");
                                return;
                            }
                        },
                        InEvent::GetAssigned(full_shard_id) => {
                            let shard = self.get_shard(&full_shard_id);
                            if let Err(_) = connection.output.send(OutEvent::AssignedShard { full_shard_id, shard: shard.cloned() }).await {
                                error!("`connection.output` is closed, shuttung down data memory");
                                return;
                            }
                        },
                        // data recollection
                        InEvent::RecollectData(data_id) => {
                            match self.currently_assembled.entry(data_id.clone()) {
                                // requests were already sent, just join the waiting list
                                hash_map::Entry::Occupied(_) => (),
                                hash_map::Entry::Vacant(v) => {
                                    for (owner, shard) in &self.distribution {
                                        if let Err(_) = connection.output.send(OutEvent::RequestAssigned{
                                            full_shard_id: (data_id.clone(), shard.clone()),
                                            location: owner.clone()
                                        }).await {
                                            error!("`connection.output` is closed, shuttung down data memory");
                                            return;
                                        }
                                    }
                                    v.insert(HashMap::new());
                                },
                            }
                        },
                        InEvent::HandleRequested { full_shard_id, shard } => {
                            if let Some(received_shards) = self.currently_assembled.get_mut(&full_shard_id.0) {
                                if !received_shards.contains_key(&full_shard_id.1) {
                                    received_shards.insert(full_shard_id.1, shard);
                                    if received_shards.len()
                                        >= self.encoding.settings()
                                            .data_shards_sufficient
                                            .try_into()
                                            .expect("# of shards sufficient is too large")
                                    {
                                        // # of shards is sufficient to reassemble
                                        let shards = self.currently_assembled.remove(&full_shard_id.0).expect("Just had this entry");
                                        let data = match self.encoding.decode(shards) {
                                            Ok(data) => data,
                                            Err(e) => {
                                                // todo: handle better
                                                warn!("Collected enough shards but failed to decode: {}", e);
                                                continue;
                                            },
                                        };
                                        if let Err(_) = connection.output.send(OutEvent::FinishedRecollection { data_id: full_shard_id.0, data: data.clone() }).await {
                                            error!("`connection.output` is closed, shuttung down data memory");
                                            return;
                                        }
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
                data_request = self.bus.shard_requests.recv() => {
                    let Some((data_id, response_handle)) = data_request else {
                        error!("memory bus is closed, shuttung down data memory");
                        return;
                    };
                    let shards: Vec<_> = self.local_storage.get(&data_id)
                        .map(|mapping| mapping
                            .values()
                            .into_iter()
                            .cloned()
                            .collect())
                        .unwrap_or_default();
                    if let Err(e) = response_handle.send(shards.get(0).cloned()) {
                        warn!("response handle for memory bus request is closed, shouldn't happen \
                            but let's try to continue operation");
                    }
                }
            }
        }
    }
}

pub struct DistributedDataMemory {
    uninit: UninitializedDataMemory,
}

impl DistributedDataMemory {
    pub fn new(bus: MemoryBus, encoding_settings: reed_solomon::Settings) -> Self {
        Self {
            uninit: UninitializedDataMemory::new(bus, encoding_settings),
        }
    }

    pub async fn run(self, mut connection: ModuleChannelServer<Module>) {
        let Some(init) = self.uninit.run(&mut connection).await else {
            return;
        };
        init.run(&mut connection).await;
    }
}
