//! Data storage and localization.
//! todo: revisit docs
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

use std::collections::{hash_map, HashMap, HashSet};

use libp2p::PeerId;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::logging_helpers::Targets;
use crate::module::ModuleChannelServer;
use crate::{
    encoding::{
        reed_solomon::{self, ReedSolomonWrapper},
        DataEncoding,
    },
    types::{Data, Shard, Sid, Vid},
};

pub struct Module;

impl crate::module::Module for Module {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type SharedState = ();
}

pub type FullShardId = (Vid, Sid);

#[derive(Debug, Clone)]
pub enum OutEvent {
    // Ready to operate
    Initialized,

    // Data distribution
    /// 1. Prepare to serve the shards to nodes
    /// - (server node) Done!
    PreparedServiceResponse(Vid),
    /// 2. The nodes see storage request transaction and pull assigned shards
    /// - (pulling node) this node requests a served shard from author of
    /// `StorageRequest` tx
    ServeShardRequest(FullShardId, PeerId),
    /// 2. The nodes see storage request transaction and pull assigned shards
    /// - (server node) The shard is sent (not considered distributed yet!)
    ServeShardResponse(FullShardId, Option<Shard>),
    /// 2. The nodes see storage request transaction and pull assigned shards
    /// - this node now stores the assigned shard, need to notify it in consensus!
    AssignedStoreSuccess(FullShardId),
    /// 3. Enough storage confirmations were seen, consider the data to be distributed
    /// successfully (though continue to serve remaining)
    DistributionSufficient(Vid),
    /// 3. All storage confirmations were seen, full data reliability is reached.
    DistributionFull(Vid),

    // Assigned & locally stored data
    /// (server) Return requested assigned shard
    AssignedResponse(FullShardId, Option<Shard>),
    /// (requester) Requesting the shard from the peer
    AssignedRequest(FullShardId, PeerId),
    /// List of successfully stored data ids
    ListDistributed(Vec<(Vid, HashMap<Sid, PeerId>)>),

    // data recollection
    /// Successfully assembled data, ready to provide it to the user
    RecollectResponse(Result<(Vid, Data), RecollectionError>),
}

#[derive(Debug, Clone, Error)]
pub enum RecollectionError {
    #[error(
        "Data id requested is not known to be stored in the system at all. \
    You might need to wait for it to appear"
    )]
    UnkonwnDataId,
    #[error("The data is not fully distributed, hopefully 'yet'")]
    NotEnoughShards,
}

#[derive(Debug, Clone)]
pub enum InEvent {
    Initialize {
        distribution: Vec<(PeerId, Sid)>,
    },

    // Data distribution
    /// 1. Prepare to serve the shards to nodes
    /// (server node)
    PrepareServiceRequest {
        data_id: Vid,
        data: Data,
    },
    /// 2. The nodes see storage request transaction and pull assigned shards
    /// - (pulling node) this node sees the storage request transaction authored
    /// by a certain peer
    StorageRequestTx(Vid, PeerId),
    /// 2. The nodes see storage request transaction and pull assigned shards
    /// - (pulling node) The shard is sent (not considered distributed yet!)
    ServeShardResponse(FullShardId, Option<Shard>),
    /// 2. The nodes see storage request transaction and pull assigned shards
    /// - (server node) the request for pull came to this node
    ServeShardRequest(FullShardId),
    /// 2. The nodes see storage request transaction and pull assigned shards
    /// - received confirmation from consensus that the node announced
    /// successful storage of this shard!
    StoreConfirmed {
        full_shard_id: FullShardId,
        location: PeerId,
    },

    // Already distributed & stored data
    /// (server) Give shard assigned to the node
    AssignedRequest(FullShardId),
    /// (requester) Receive previously requested shard
    AssignedResponse(FullShardId, Option<Shard>),
    /// List of successfully stored data ids
    ListDistributed,

    // data recollection
    /// Recollect data with given id, request by user
    RecollectRequest(Vid),

    // program execution updates
    PeerShardsActualized {
        peer: PeerId,
        updated_data_ids: Vec<Vid>,
    },
}

pub struct MemoryBus {
    reads: mpsc::Receiver<(Vid, oneshot::Sender<Option<Shard>>)>,
    /// Store new value of the assigned shard
    writes: mpsc::Receiver<(Vid, Shard)>,
}

impl MemoryBus {
    pub fn new(
        reads: mpsc::Receiver<(Vid, oneshot::Sender<Option<Shard>>)>,
        writes: mpsc::Receiver<(Vid, Shard)>,
    ) -> Self {
        Self { reads, writes }
    }

    pub fn channel(buffer: usize) -> (Self, crate::processor::single_threaded::MemoryBus) {
        let reads = mpsc::channel(buffer);
        let writes = mpsc::channel(buffer);
        let this_end = Self::new(reads.1, writes.1);
        let other_end = crate::processor::single_threaded::MemoryBus::new(reads.0, writes.0);
        (this_end, other_end)
    }
}

struct UninitializedDataMemory {
    bus: MemoryBus,
    encoding: ReedSolomonWrapper,
    local_id: PeerId,
}

impl UninitializedDataMemory {
    fn new(local_id: PeerId, bus: MemoryBus, encoding_settings: reed_solomon::Settings) -> Self {
        let encoding = ReedSolomonWrapper::new(encoding_settings);
        Self {
            bus,
            encoding,
            local_id,
        }
    }

    /// `true` == valid
    fn verify_distribution(&self, distribution: &HashMap<PeerId, Sid>) -> bool {
        let shard_ids: HashSet<_> = distribution.values().collect();
        let expected_ids_owned: HashSet<_> = (0..)
            .take(
                self.encoding
                    .settings()
                    .data_shards_total
                    .try_into()
                    .unwrap(),
            )
            .map(|i| Sid(i))
            .collect();
        let expected_ids: HashSet<_> = expected_ids_owned.iter().collect();
        shard_ids == expected_ids
    }

    fn initialize(self, distribution: HashMap<PeerId, Sid>) -> InitializedDataMemory {
        InitializedDataMemory {
            local_storage: HashMap::new(),
            to_distribute: HashMap::new(),
            currently_assembled: HashMap::new(),
            distribution,
            local_id: self.local_id,
            bus: self.bus,
            encoding: self.encoding,
            data_known_locations: HashMap::new(),
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
                            debug!(target: Targets::StorageInitialization.into_str(), "Initializing storage...");
                            let distribution = distribution.into_iter().collect();
                            if !self.verify_distribution(&distribution) {
                                warn!("received distribution doesn't match expected pattern; \
                                expected to have a peer for each shard id from 0 to <total shard number>; \
                                got: {:?}", distribution);
                                continue;
                            }
                            info!("storage initialized, ready");
                            debug!(target: Targets::StorageInitialization.into_str(), "Notifying the user");
                            if let Err(_) = connection.output.send(OutEvent::Initialized).await {
                                error!("`connection.output` is closed, shuttung down data memory");
                                return None;
                            }
                            return Some(self.initialize(distribution))
                        },
                        InEvent::StoreConfirmed {
                            full_shard_id: _,
                            location: _,
                        }
                        | InEvent::PrepareServiceRequest {
                            data_id: _,
                            data: _,
                        }
                        | InEvent::StorageRequestTx(_, _)
                        | InEvent::ServeShardRequest(_)
                        | InEvent::ServeShardResponse(_, _)
                        | InEvent::AssignedRequest(_)
                        | InEvent::ListDistributed
                        | InEvent::RecollectRequest(_)
                        | InEvent::AssignedResponse(_, _)
                        | InEvent::PeerShardsActualized { peer: _, updated_data_ids: _ } => warn!("have not initialized storage, ignoring request {:?}", in_event),
                    }

                }
                data_request = self.bus.reads.recv() => {
                    let Some(_) = data_request else {
                        error!("memory bus is closed, shuttung down data memory");
                        return None;
                    };
                    warn!("have not initialized storage, ignoring read request from memory bus");
                }
                write_request = self.bus.writes.recv() => {
                    let Some(_) = write_request else {
                        error!("memory bus is closed, shuttung down data memory");
                        return None;
                    };
                    warn!("have not initialized storage, ignoring write request from memory bus");
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
/// Created with [`UninitializedDataMemory::initialize()`]
///
/// Use [`Self::run()`] to operate.
struct InitializedDataMemory {
    // `HashMap<Sid, Shard>` because in the future we might store multiple shards on each peer
    local_storage: HashMap<Vid, HashMap<Sid, Shard>>,
    to_distribute: HashMap<Vid, HashMap<Sid, Shard>>,
    currently_assembled: HashMap<Vid, HashMap<Sid, Shard>>,
    /// `None` means it is the memory (and the system) is not active
    distribution: HashMap<PeerId, Sid>,
    data_known_locations: HashMap<Vid, HashMap<Sid, PeerId>>,
    local_id: PeerId,
    bus: MemoryBus,
    encoding: ReedSolomonWrapper,
}

impl InitializedDataMemory {
    fn assigned_shard_id(&self) -> Option<&Sid> {
        self.distribution.get(&self.local_id)
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

    // todo: make some removal mechanism
    /// Remove shard from the local storage and return it (if there was any)
    #[allow(unused)]
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
        if let Some(expected_shard_id) = self.distribution.get(&location) {
            if expected_shard_id != &full_shard_id.1 {
                warn!(
                    "observed shard at unexpected location. observed at {:?}, expected at {:?}",
                    &full_shard_id.1, expected_shard_id
                );
            }
        } else {
            warn!("observed location does not appear in known data distribution. shouldn't happen");
        }

        // need to track location to count successful distributions
        let shards = self
            .data_known_locations
            .entry(full_shard_id.0)
            .or_default();
        shards.insert(full_shard_id.1, location);
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

enum HandleResult {
    Ok,
    Abort,
}

impl InitializedDataMemory {
    async fn handle_serve_shard_response(
        &mut self,
        full_shard_id: FullShardId,
        shard: Option<Shard>,
        connection: &mut ModuleChannelServer<Module>,
    ) -> HandleResult {
        let Some(shard) = shard else {
            warn!("peer that announced event distribution doesn't have shard assigned to us. strange but ok.");
            return HandleResult::Ok;
        };
        debug!(
            target: Targets::DataDistribution.into_str(),
            "Storing shard {:?}", full_shard_id
        );
        self.store_shard(full_shard_id.clone(), shard);
        if let Err(_) = connection
            .output
            .send(OutEvent::AssignedStoreSuccess(full_shard_id))
            .await
        {
            error!("`connection.output` is closed, shuttung down data memory");
            return HandleResult::Abort;
        }
        return HandleResult::Ok;
    }

    async fn handle_assigned_response(
        &mut self,
        full_shard_id: FullShardId,
        shard: Option<Shard>,
        connection: &mut ModuleChannelServer<Module>,
    ) -> HandleResult {
        let Some(shard) = shard else {
            warn!("Peer that announced that it stores assigned shard doesn't have it. Misbehaviour??");
            return HandleResult::Ok;
        };
        let Some(received_shards) = self.currently_assembled.get_mut(&full_shard_id.0) else {
            debug!("received shard was likely already assembled, skipping");
            return HandleResult::Ok;
        };
        if !received_shards.contains_key(&full_shard_id.1) {
            received_shards.insert(full_shard_id.1, shard);
            let sufficient_shards_n = self
                .encoding
                .settings()
                .data_shards_sufficient
                .try_into()
                .expect("# of shards sufficient is too large");
            if received_shards.len() >= sufficient_shards_n {
                // # of shards is sufficient to reassemble
                let shards = self
                    .currently_assembled
                    .remove(&full_shard_id.0)
                    .expect("Just had this entry");
                let data = match self.encoding.decode(shards) {
                    Ok(data) => data,
                    Err(e) => {
                        // todo: handle better
                        warn!("Collected enough shards but failed to decode: {}", e);
                        return HandleResult::Ok;
                    }
                };
                if let Err(_) = connection
                    .output
                    .send(OutEvent::RecollectResponse(Ok((
                        full_shard_id.0,
                        data.clone(),
                    ))))
                    .await
                {
                    error!("`connection.output` is closed, shuttung down data memory");
                    return HandleResult::Abort;
                }
            }
        } else {
            // not sure when it's possible
            warn!("received shard that is already present, weird");
        }
        return HandleResult::Ok;
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
                    match in_event {
                        InEvent::Initialize { distribution: _ } => {
                            warn!("received `InitializeStorage` transaction but storage was already initialized. ignoring");
                        },
                        // initial distribution
                        InEvent::PrepareServiceRequest { data_id, data } => {
                            debug!(
                                target: Targets::DataDistribution.into_str(),
                                "Encoding data {:?} into shards", data_id
                            );
                            let shards = self.encoding.encode(data).expect(
                                "Encoding settings are likely incorrect, \
                                all problems should've been caught at type-level"
                            );
                            debug!(
                                target: Targets::DataDistribution.into_str(),
                                "Encoded into {} shards", shards.len()
                            );
                            self.prepare_to_serve_shards(data_id.clone(), shards);
                            debug!(
                                target: Targets::DataDistribution.into_str(),
                                "Prepared to serve shards of {:?}", data_id
                            );
                            if let Err(_) = connection.output.send(
                                OutEvent::PreparedServiceResponse(data_id)
                            ).await {
                                error!("`connection.output` is closed, shuttung down data memory");
                                return;
                            }
                        },
                        InEvent::StorageRequestTx(data_id, author) => {
                            debug!(
                                target: Targets::DataDistribution.into_str(),
                                "Checking assigned shard id"
                            );
                            let Some(assigned_sid) = self.assigned_shard_id() else {
                                debug!("see a storage request transaction from consensus, \
                                but no shard ids are assigned to this peer. ignoring.");
                                continue;
                            };
                            if author == self.local_id {
                                let full_shard_id = (data_id, assigned_sid.clone());
                                let shard = self.serve_shard(&full_shard_id);
                                // imitate incoming `ServeShardResponse`
                                match self.handle_serve_shard_response(full_shard_id, shard, connection).await {
                                    HandleResult::Ok => (),
                                    HandleResult::Abort => {
                                        connection.shutdown.cancel();
                                        return
                                    },
                                }
                            }
                            else {
                                debug!(
                                    target: Targets::DataDistribution.into_str(),
                                    "Requesting shard {:?} for {:?}", assigned_sid, data_id
                                );
                                if let Err(_) = connection.output.send(
                                    OutEvent::ServeShardRequest((data_id, assigned_sid.clone()), author)
                                ).await {
                                    error!("`connection.output` is closed, shuttung down data memory");
                                    return;
                                }
                            }
                        },
                        InEvent::ServeShardRequest(full_shard_id) => {
                            let shard = self.serve_shard(&full_shard_id);
                            if let Err(_) = connection.output.send(OutEvent::ServeShardResponse(full_shard_id, shard)).await {
                                error!("`connection.output` is closed, shuttung down data memory");
                                return;
                            }
                        },
                        // assigned data
                        InEvent::ServeShardResponse(full_shard_id, shard) => {
                            match self.handle_serve_shard_response(full_shard_id, shard, connection).await {
                                HandleResult::Ok => (),
                                HandleResult::Abort => {
                                    connection.shutdown.cancel();
                                    return
                                },
                            }
                        },
                        InEvent::StoreConfirmed { full_shard_id, location } => {
                            debug!(
                                target: Targets::DataDistribution.into_str(),
                                "Observed shard {:?} location: {:?}", full_shard_id, location
                            );
                            self.observe_new_location(full_shard_id.clone(), location);
                            let Some(this_data_locations) = self.data_known_locations.get(&full_shard_id.0) else {
                                warn!("bug in tracking data locations, new locations are not registered for some reason");
                                continue;
                            };
                            let sufficient_shards: usize = self.encoding.settings().data_shards_sufficient.try_into().unwrap();
                            let total_shards: usize = self.encoding.settings().data_shards_total.try_into().unwrap();
                            debug!(
                                target: Targets::DataDistribution.into_str(),
                                "Checking if enough shards were distributed (threshold = {}/{}, have {})", sufficient_shards, total_shards, this_data_locations.len()
                            );
                            if this_data_locations.len() == sufficient_shards {
                                debug!(
                                    target: Targets::DataDistribution.into_str(),
                                    "Enough shards were distributed, reporting to the user as a sufficient distribution"
                                );
                                if let Err(_) = connection.output.send(
                                    OutEvent::DistributionSufficient(full_shard_id.0.clone())
                                ).await {
                                    error!("`connection.output` is closed, shuttung down data memory");
                                    return;
                                }
                            }
                            if this_data_locations.len() == total_shards {
                                debug!(
                                    target: Targets::DataDistribution.into_str(),
                                    "All shards were distributed, reporting to the user as a full distribution"
                                );
                                self.to_distribute.remove(&full_shard_id.0);
                                if let Err(_) = connection.output.send(
                                    OutEvent::DistributionFull(full_shard_id.0)
                                ).await {
                                    error!("`connection.output` is closed, shuttung down data memory");
                                    return;
                                }
                            }
                        },
                        InEvent::AssignedRequest(full_shard_id) => {
                            let shard = self.get_shard(&full_shard_id);
                            if let Err(_) = connection.output.send(OutEvent::AssignedResponse(full_shard_id, shard.cloned())).await {
                                error!("`connection.output` is closed, shuttung down data memory");
                                return;
                            }
                        },
                        InEvent::ListDistributed => {
                            let sufficient_shards = self.encoding.settings().data_shards_sufficient;
                            let distributed = self.data_known_locations.iter()
                                .filter(|(_, locations)| locations.len() >= sufficient_shards.try_into().unwrap())
                                .map(|(a, b)| (a.clone(), b.clone()))
                                .collect();
                            if let Err(_) = connection.output.send(OutEvent::ListDistributed(distributed)).await {
                                error!("`connection.output` is closed, shuttung down data memory");
                                return;
                            }
                        }
                        // data recollection
                        InEvent::RecollectRequest(data_id) => {
                            let Some(known_locations) = self.data_known_locations.get(&data_id) else {
                                if let Err(_) = connection.output.send(
                                    OutEvent::RecollectResponse(Err(RecollectionError::UnkonwnDataId))
                                ).await {
                                    error!("`connection.output` is closed, shuttung down data memory");
                                    return;
                                }
                                continue;
                            };
                            let sufficient_shards_n = self.encoding.settings()
                                .data_shards_sufficient
                                .try_into()
                                .expect("# of shards sufficient is too large");
                            if known_locations.len() < sufficient_shards_n {
                                if let Err(_) = connection.output.send(
                                    OutEvent::RecollectResponse(Err(RecollectionError::NotEnoughShards))
                                ).await {
                                    error!("`connection.output` is closed, shuttung down data memory");
                                    return;
                                }
                                continue;
                            }
                            match self.currently_assembled.entry(data_id.clone()) {
                                // requests were already sent, just join the waiting list
                                hash_map::Entry::Occupied(_) => (),
                                hash_map::Entry::Vacant(v) => {
                                    v.insert(HashMap::new());
                                },
                            }
                            for (shard_id, owner) in known_locations.clone() {
                                if owner == self.local_id {
                                    let full_shard_id = (data_id.clone(), shard_id);
                                    let shard = self.get_shard(&full_shard_id).cloned();
                                    // imitate incoming `ServeShardResponse`
                                    match self.handle_assigned_response(full_shard_id, shard, connection).await {
                                        HandleResult::Ok => (),
                                        HandleResult::Abort => {
                                            connection.shutdown.cancel();
                                            return
                                        },
                                    }
                                }
                                else {
                                    if let Err(_) = connection.output.send(OutEvent::AssignedRequest(
                                        (data_id.clone(), shard_id),
                                        owner.clone()
                                    )).await {
                                        error!("`connection.output` is closed, shuttung down data memory");
                                        return;
                                    }
                                }
                            }
                        },
                        InEvent::AssignedResponse(full_shard_id, shard) => {
                            match self.handle_assigned_response(full_shard_id, shard, connection).await {
                                HandleResult::Ok => (),
                                HandleResult::Abort => {
                                    connection.shutdown.cancel();
                                    return
                                },
                            }
                        },
                        InEvent::PeerShardsActualized { peer, updated_data_ids } => {
                            let Some(expected_shard_id) = self.distribution.get(&peer) else {
                                warn!("Received program execution notification but the peer \
                                is not assigned any shards. It shouldn't've send it.");
                                return;
                            };
                            for data_id in updated_data_ids {
                                let shards = self
                                    .data_known_locations
                                    .entry(data_id)
                                    .or_default();
                                shards.insert(expected_shard_id.clone(), peer);
                            }
                        }
                    }
                }
                data_request = self.bus.reads.recv() => {
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
                            but let's try to continue operation; {:?}", e);
                    }
                }
                write_request = self.bus.writes.recv() => {
                    let Some((data_id, shard)) = write_request else {
                        error!("memory bus is closed, shuttung down data memory");
                        return;
                    };
                    let Some(assigned_sid) = self.assigned_shard_id() else {
                        warn!("received write request from memory bus but no shards are assigned to this node. likely a bug, just ignoring");
                        continue;
                    };
                    self.store_shard((data_id, assigned_sid.clone()), shard);
                }
            }
        }
    }
}

/// Data memory that first waits for initialization
/// event and then starts operating normally
pub struct DistributedDataMemory {
    uninit: UninitializedDataMemory,
}

impl DistributedDataMemory {
    pub fn new(
        local_id: PeerId,
        bus: MemoryBus,
        encoding_settings: reed_solomon::Settings,
    ) -> Self {
        Self {
            uninit: UninitializedDataMemory::new(local_id, bus, encoding_settings),
        }
    }

    async fn run_memory(self, mut connection: ModuleChannelServer<Module>) {
        let Some(init) = self.uninit.run(&mut connection).await else {
            return;
        };
        debug!(target: Targets::StorageInitialization.into_str(), "Initialized data storage, ready for operation");
        init.run(&mut connection).await;
    }

    pub async fn run(self, connection: ModuleChannelServer<Module>) {
        let shutdown = connection.shutdown.clone();
        tokio::select! {
            _ = self.run_memory(connection) => { }
            _ = shutdown.cancelled() => {
                info!("received cancel signal, shutting down data memory");
                return;
            }
        }
    }
}
