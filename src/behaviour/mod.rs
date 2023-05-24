//! TODO: check accepts_input()
use std::{
    collections::{HashMap, HashSet, VecDeque},
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use futures::{pin_mut, Future};
use libp2p::{
    swarm::{
        derive_prelude::ConnectionEstablished,
        dial_opts::{DialOpts, PeerCondition},
        ConnectionClosed, FromSwarm, NetworkBehaviour, NotifyHandler, ToSwarm,
    },
    PeerId,
};
use libp2p_request_response::RequestId;
use rand::Rng;

use thiserror::Error;
use tokio::{
    sync::Notify,
    time::{sleep, Sleep},
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    channel_log_recv, channel_log_send,
    consensus::{self, Transaction},
    data_memory, instruction_storage,
    logging_helpers::Targets,
    processor::{
        single_threaded::{self},
        Program,
    },
    protocol::{
        self,
        one_shot::{InnerMessage, SimpleMessage, SwarmOneShot},
        Request,
    },
};
use crate::{
    module::{ModuleChannelClient, ModuleChannelServer},
    types::Sid,
};
pub use module::{InEvent, Module, OutEvent};

pub type ToSwarmEvent = Result<Event, Error>;

#[derive(Debug)]
pub enum Event {}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Cannot continue behaviour operation. Shutdown (and fresh start?) is the most desirable outcome.")]
    UnableToOperate,
    #[error("Received signal to shut down the module")]
    CancelSignal,
}

mod module {
    use std::collections::HashMap;

    use libp2p::PeerId;

    use crate::{
        data_memory,
        processor::{Instructions, ProgramIdentifier},
        types::{Data, Sid, Vid},
    };

    pub struct Module;

    impl crate::module::Module for Module {
        type InEvent = InEvent;
        type OutEvent = OutEvent;
        type SharedState = ();
    }

    #[derive(Debug, Clone)]
    pub enum InEvent {
        // schedule program, collect data, distribute data
        ScheduleProgram(Instructions),
        Get(Vid),
        Put(Vid, Data),
        ListStored,
        InitializeStorage,
    }

    #[derive(Debug, Clone)]
    pub enum OutEvent {
        // TODO: add hash?
        ScheduleOk,
        ProgramExecuted(ProgramIdentifier),
        GetResponse(Result<(Vid, Data), data_memory::RecollectionError>),
        PutConfirmed(Vid),
        ListStoredResponse(Vec<(Vid, HashMap<Sid, PeerId>)>),
        StorageInitialized,
    }
}

struct ConnectionEventWrapper<E> {
    peer_id: libp2p::PeerId,
    _connection: libp2p::swarm::ConnectionId,
    event: E,
}

pub struct Behaviour {
    // might be useful, leave it
    #[allow(unused)]
    local_peer_id: PeerId,
    discovered_peers: VecDeque<PeerId>,

    user_interaction: ModuleChannelServer<module::Module>,
    // connections to other system components (run as separate async tasks)
    // todo: do some wrapper that'll check for timeouts and stuff. maybe also match request-response
    consensus: ModuleChannelClient<consensus::graph::Module>,
    instruction_memory: ModuleChannelClient<instruction_storage::Module>,
    data_memory: ModuleChannelClient<data_memory::Module>,
    processor: ModuleChannelClient<single_threaded::Module>,
    request_response: ModuleChannelClient<crate::request_response::Module>,

    // random gossip
    connected_peers: HashSet<PeerId>,
    rng: rand::rngs::ThreadRng,
    consensus_gossip_timer: Pin<Box<Sleep>>,
    consensus_gossip_timeout: Duration,

    // connection stuff
    oneshot_events: VecDeque<ConnectionEventWrapper<InnerMessage>>,
    pending_response: HashMap<RequestId, Request>,
    processed_requests: HashMap<
        Request,
        Vec<(
            RequestId,
            libp2p_request_response::ResponseChannel<protocol::Response>,
        )>,
    >,

    // notification to poll() to wake up and try to do some progress
    state_updated: Arc<Notify>,
}

impl Behaviour {
    pub fn new(
        local_peer_id: PeerId,
        consensus_gossip_timeout: Duration,
        user_interaction: ModuleChannelServer<module::Module>,
        consensus: ModuleChannelClient<consensus::graph::Module>,
        instruction_memory: ModuleChannelClient<instruction_storage::Module>,
        data_memory: ModuleChannelClient<data_memory::Module>,
        processor: ModuleChannelClient<single_threaded::Module>,
        request_response: ModuleChannelClient<crate::request_response::Module>,
    ) -> Self {
        Self {
            local_peer_id,
            discovered_peers: VecDeque::new(),
            user_interaction,
            consensus,
            instruction_memory,
            data_memory,
            processor,
            request_response,
            connected_peers: HashSet::new(),
            rng: rand::thread_rng(),
            consensus_gossip_timer: Box::pin(sleep(consensus_gossip_timeout)),
            consensus_gossip_timeout,
            oneshot_events: VecDeque::new(),
            pending_response: HashMap::new(),
            processed_requests: HashMap::new(),
            state_updated: Arc::new(Notify::new()),
        }
    }

    /// Notify behaviour that peer is discovered
    pub fn inject_peer_discovered(&mut self, new_peer: PeerId) {
        debug!("Discovered new peer {}", new_peer);
        self.discovered_peers.push_front(new_peer);
    }

    /// Notify behaviour that peer not discoverable and is expired according to MDNS
    pub fn inject_peer_expired(&mut self, _peer: &PeerId) {
        // Maybe add some logic later
    }
}

impl Behaviour {
    /// None if none connected
    fn get_random_peer(&mut self) -> Option<PeerId> {
        let connected = self.connected_peers.len();
        if connected == 0 {
            return None;
        }
        let range = 0..connected;
        let position = self.rng.gen_range(range);
        let mut i = self.connected_peers.iter().skip(position);
        Some(
            *i.next()
                .expect("Shouldn't have skipped more than `len-1` elements."),
        )
    }
}

macro_rules! cant_operate_error_return {
    ($($arg:tt)+) => {
        {
            error!($($arg)+);
            return Poll::Ready(libp2p::swarm::ToSwarm::GenerateEvent(Err(
                Error::UnableToOperate,
            )));
        }
    };
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = SwarmOneShot;
    type OutEvent = ToSwarmEvent;

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: libp2p::swarm::ConnectionId,
        _peer: PeerId,
        _local_addr: &libp2p::Multiaddr,
        _remote_addr: &libp2p::Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        debug!("Creating new inbound connection handler");
        Ok(SwarmOneShot::default())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: libp2p::swarm::ConnectionId,
        _peer: PeerId,
        _addr: &libp2p::Multiaddr,
        _role_override: libp2p::core::Endpoint,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        debug!("Creating new out bound connection handler");
        Ok(SwarmOneShot::default())
    }

    fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                connection_id: _,
                endpoint: _,
                failed_addresses: _,
                other_established,
            }) => {
                if other_established > 0 {
                    return;
                }
                info!("Adding peer {:?} to the list of connected", peer_id);
                if !self.connected_peers.insert(peer_id) {
                    warn!("Newly connecting peer was already in connected list, data is inconsistent (?).");
                }
            }
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                connection_id: _,
                endpoint: _,
                handler: _,
                remaining_established,
            }) => {
                if remaining_established > 0 {
                    return;
                }
                info!("Removing peer {:?} from the list of connected", peer_id);
                if !self.connected_peers.remove(&peer_id) {
                    warn!("Disconnecting peer wasn't in connected list, data is inconsistent (?).");
                }
            }
            FromSwarm::AddressChange(_)
            | FromSwarm::DialFailure(_)
            | FromSwarm::ListenFailure(_)
            | FromSwarm::NewListener(_)
            | FromSwarm::NewListenAddr(_)
            | FromSwarm::ExpiredListenAddr(_)
            | FromSwarm::ListenerError(_)
            | FromSwarm::ListenerClosed(_)
            | FromSwarm::NewExternalAddr(_)
            | FromSwarm::ExpiredExternalAddr(_) => (),
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: libp2p::PeerId,
        connection: libp2p::swarm::ConnectionId,
        event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        self.oneshot_events.push_front(ConnectionEventWrapper {
            peer_id,
            _connection: connection,
            event,
        });
        self.state_updated.notify_one();
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        _params: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<libp2p::swarm::ToSwarm<Self::OutEvent, libp2p::swarm::THandlerInEvent<Self>>>
    {
        {
            let shutdown_signal = self.user_interaction.shutdown.cancelled();
            pin_mut!(shutdown_signal);
            match shutdown_signal.poll(cx) {
                Poll::Ready(_) => {
                    return Poll::Ready(ToSwarm::GenerateEvent(Err(Error::CancelSignal)))
                }
                Poll::Pending => (),
            }
        }

        trace!("Checking discovered peers to connect");
        match self.discovered_peers.pop_back() {
            Some(peer) => {
                debug!("Discovered (new) peer, trying to negotiate protocol");
                let opts = DialOpts::peer_id(peer)
                    .condition(PeerCondition::Disconnected)
                    .build();
                return Poll::Ready(ToSwarm::Dial { opts });
            }
            None => trace!("No new peers found"),
        }

        // todo: reconsider ordering
        loop {
            let state_updated_notification = self.state_updated.notified();
            pin_mut!(state_updated_notification);
            // Maybe break on Pending?
            let _ = state_updated_notification.poll(cx);

            match self.oneshot_events.pop_back() {
                // serve shard, recieve shard, recieve gossip,
                Some(s) => {
                    match s.event {
                        InnerMessage::Rx(SimpleMessage(protocol::Simple::GossipGraph(sync))) => {
                            channel_log_recv!(
                                "network.simple",
                                format!("GossipGraph(from: {:?})", &s.peer_id)
                            );
                            let send_future =
                                self.consensus
                                    .input
                                    .send(consensus::graph::InEvent::ApplySync {
                                        from: s.peer_id,
                                        sync,
                                    });
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => channel_log_send!("consensus.input", format!("ApplySync(from: {})", s.peer_id)),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing will apply received sync. for now fail fast to see this."),
                            }
                        }
                        InnerMessage::Sent => trace!("Sent simple successfully"),
                    }
                    continue;
                }
                None => (),
            }
            break;
        }

        match self.data_memory.output.poll_recv(cx) {
            Poll::Ready(Some(event)) => match event {
                data_memory::OutEvent::ServeShardRequest(full_shard_id, location) => {
                    // todo: separate workflow for `from` == `local_peer_id`
                    if location == self.local_peer_id {
                        info!("Requesting from myself");
                    }
                    debug!(
                        target: Targets::DataDistribution.into_str(),
                        "Sending serve request for {:?}", full_shard_id
                    );
                    let request = protocol::Request::ServeShard(full_shard_id);
                    channel_log_send!("network.request", format!("{:?}", request));
                    let send_future = self.request_response
                        .input
                        .send(crate::request_response::InEvent::MakeRequest{
                            request: request.clone(),
                            to: location,
                        });
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("network.request", format!("{:?}", request)),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `network.request` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`network.request` queue is full. continuing will drop our request. for now fail fast to see this."),
                    }
                },
                data_memory::OutEvent::ServeShardResponse(full_shard_id, shard) => {
                    debug!(
                        target: Targets::DataDistribution.into_str(),
                        "Responding to ServeShard request for {:?}, shard is_some={:?}", full_shard_id, shard.is_some()
                    );
                    let request = protocol::Request::ServeShard(full_shard_id.clone());
                    let response = protocol::Response::ServeShard(shard);
                    let waiting_for_response = self.processed_requests.remove(&request).unwrap_or_default();
                    for (request_id, sender) in waiting_for_response {
                        debug!(
                            target: Targets::DataDistribution.into_str(),
                            "Serving shard {:?} for request {:?}", full_shard_id, request_id
                        );
                        channel_log_send!("network.response", format!("{:?}", request));
                        let send_future = self.request_response
                            .input
                            .send(crate::request_response::InEvent::Respond {
                                request_id: request_id,
                                channel: sender,
                                response: response.clone(),
                            });
                        pin_mut!(send_future);
                        match send_future.poll(cx) {
                            Poll::Ready(Ok(_)) => channel_log_send!("network.response", format!("{:?}", response)),
                            Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `network.response` was closed. cannot operate without this module."),
                            Poll::Pending => cant_operate_error_return!("`network.response` queue is full. continuing will ignore someone's request. for now fail fast to see this."),
                        }
                    }
                },
                data_memory::OutEvent::AssignedStoreSuccess(full_shard_id) => {
                    debug!(
                        target: Targets::DataDistribution.into_str(),
                        "Notifying other nodes that we store shard {:?} via consensus tx", full_shard_id
                    );
                    let event = consensus::graph::InEvent::ScheduleTx(Transaction::Stored(full_shard_id.0, full_shard_id.1));
                    let send_future = self.consensus.input.send(event.clone());
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("consensus.input", format!("{:?}", event)),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing will not notify other peers on storing shard. for now fail fast to see this."),
                    }
                }
                data_memory::OutEvent::AssignedResponse(full_shard_id, shard) => {
                    let request = protocol::Request::GetShard(full_shard_id);
                    let response = protocol::Response::GetShard(shard);
                    let waiting_for_response = self.processed_requests.remove(&request).unwrap_or_default();
                    for (request_id, sender) in waiting_for_response {
                        channel_log_send!("network.response", format!("{:?}", request));
                        let send_future = self.request_response
                            .input
                            .send(crate::request_response::InEvent::Respond {
                                request_id: request_id,
                                channel: sender,
                                response: response.clone(),
                            });
                        pin_mut!(send_future);
                        match send_future.poll(cx) {
                            Poll::Ready(Ok(_)) => channel_log_send!("network.response", format!("{:?}", response)),
                            Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `network.response` was closed. cannot operate without this module."),
                            Poll::Pending => cant_operate_error_return!("`network.response` queue is full. continuing will ignore someone's request. for now fail fast to see this."),
                        }
                    }
                },
                data_memory::OutEvent::DistributionSuccess(data_id) => {
                    let event = module::OutEvent::PutConfirmed(data_id);
                    let send_future = self.user_interaction.output.send(event.clone());
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("user_interaction.input", format!("{:?}", event)),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `user_interaction.output` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`user_interaction.output` queue is full. continuing will leave user request unanswered. for now fail fast to see this."),
                    }
                },
                data_memory::OutEvent::ListDistributed(list) => {
                    let send_future = self.user_interaction.output.send(
                        module::OutEvent::ListStoredResponse(list)
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("user_interaction.input", "ListStoredResponse"),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `user_interaction.output` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`user_interaction.output` queue is full. continuing will leave user request unanswered. for now fail fast to see this."),
                    }
                },
                data_memory::OutEvent::PreparedServiceResponse(data_id) => {
                    debug!(
                        target: Targets::DataDistribution.into_str(),
                        "Placing storage request for {:?} onto consensus to notify peers", data_id
                    );
                    let event = consensus::graph::InEvent::ScheduleTx(Transaction::StorageRequest { data_id });
                    let send_future = self.consensus.input.send(event.clone());
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("consensus.input", format!("{:?}", event)),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing might not fulfill user's expectations. for now fail fast to see this."),
                    }
                },
                data_memory::OutEvent::AssignedRequest(full_shard_id, location) => {
                    let request = protocol::Request::GetShard(full_shard_id);
                    channel_log_send!("network.request", format!("{:?}", request));
                    let send_future = self.request_response
                        .input
                        .send(crate::request_response::InEvent::MakeRequest{
                            request: request.clone(),
                            to: location,
                        });
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("network.request", format!("{:?}", request)),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `network.request` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`network.request` queue is full. continuing will drop our request. for now fail fast to see this."),
                    }

                }
                data_memory::OutEvent::RecollectResponse(response) => {
                    let send_future = self.user_interaction.output.send(
                        module::OutEvent::GetResponse(response)
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("user_interaction.input", "GetResponse"),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `user_interaction.output` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`user_interaction.output` queue is full. continuing will leave user request unanswered. for now fail fast to see this."),
                    }
                },
                data_memory::OutEvent::Initialized => {
                    let send_future = self.user_interaction.output.send(
                        module::OutEvent::StorageInitialized
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("user_interaction.input", "StorageInitialized"),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `user_interaction.output` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`user_interaction.output` queue is full. continuing will leave user request unanswered. for now fail fast to see this."),
                    }
                },
            },
            Poll::Ready(None) => cant_operate_error_return!("other half of `data_memory.output` was closed. cannot operate without this module."),
            Poll::Pending => (),
        }

        // TODO: check if futures::select! is applicable to avoid starvation (??)
        match self.user_interaction.input.poll_recv(cx) {
            Poll::Ready(Some(event)) => match event {
                InEvent::ScheduleProgram(instructions) => {
                    let send_future = self.consensus.input.send(
                        consensus::graph::InEvent::ScheduleTx(Transaction::Execute(instructions))
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("consensus.input", "ScheduleTx(Execute(_))"),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing might not fulfill user's expectations. for now fail fast to see this."),
                    }
                    let send_future = self.user_interaction.output.send(
                        module::OutEvent::ScheduleOk
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("user_interaction.input", "ScheduleOk"),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `user_interaction.output` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`user_interaction.output` queue is full. continuing will leave user request unanswered. for now fail fast to see this."),
                    }
                },
                InEvent::Get(data_id) => {
                    let event = data_memory::InEvent::RecollectRequest(data_id);
                    let send_future = self.data_memory.input.send(event.clone());
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("data_memory.input", format!("{:?}", event)),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing might not fulfill user's expectations. for now fail fast to see this."),
                    }
                },
                InEvent::Put(data_id, data) => {
                    debug!(target: Targets::DataDistribution.into_str(), "Starting distribution process of data {:?}", data_id);
                    let send_future = self.data_memory.input.send(
                        data_memory::InEvent::PrepareServiceRequest { data_id: data_id.clone(), data }
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("data_memory.input", format!("PrepareServiceRequest({:?})", data_id)),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing might not fulfill user's expectations. for now fail fast to see this."),
                    }
                },
                InEvent::ListStored => {
                    let send_future = self.data_memory.input.send(
                        data_memory::InEvent::ListDistributed
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("data_memory.input", "ListDistributed"),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing might not fulfill user's expectations. for now fail fast to see this."),
                    }
                },
                InEvent::InitializeStorage => {
                    debug!(target: Targets::StorageInitialization.into_str(), "Starting storage initialization, getting list of known peers");
                    let send_future = self.consensus.input.send(
                        consensus::graph::InEvent::KnownPeersRequest
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("consensus.input", "KnownPeersRequest"),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing will not notify other peers on program execution. for now fail fast to see this."),
                    }
                }
            },
            Poll::Ready(None) => cant_operate_error_return!("`user_interaction.input` (at client) was closed. not intended to operate without interaction with user."),
            Poll::Pending => (),
        }

        match self.processor.output.poll_recv(cx) {
            Poll::Ready(Some(single_threaded::OutEvent::FinishedExecution { program_id, results })) => {
                debug!("Finished executing program {:?}\nResults: {:?}", program_id.clone(), results);
                let event = consensus::graph::InEvent::ScheduleTx(Transaction::Executed(program_id));
                let send_future = self.consensus.input.send(event.clone());
                pin_mut!(send_future);
                match send_future.poll(cx) {
                    Poll::Ready(Ok(_)) => channel_log_send!("consensus.input", format!("{:?}", event)),
                    Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                    Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing will not notify other peers on program execution. for now fail fast to see this."),
                }
            }
            Poll::Ready(None) => cant_operate_error_return!("other half of `instruction_memory.output` was closed. cannot operate without this module."),
            Poll::Pending => (),
        }

        if self.processor.accepts_input() {
            match self.instruction_memory.output.poll_recv(cx) {
                Poll::Ready(Some(event)) => {
                    match event {
                        instruction_storage::OutEvent::NextProgram(program) => {
                            let send_future = self.processor.input.send(single_threaded::InEvent::Execute(program));
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => channel_log_send!("processor.input", "Execute"),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `processor.input` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`processor.input` queue is full. continuing will skip a program for execution, which is unacceptable."),
                            }
                        }
                        instruction_storage::OutEvent::FinishedExecution(program_id) => {
                            let event = module::OutEvent::ProgramExecuted(program_id);
                            let send_future = self.user_interaction.output.send(event.clone());
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => channel_log_send!("user_interaction.input", format!("{:?}", event)),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `user_interaction.output` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`user_interaction.output` queue is full. continuing will leave user request unanswered. for now fail fast to see this."),
                            }
                        },
                    }
                }
                Poll::Ready(None) => cant_operate_error_return!("other half of `instruction_memory.output` was closed. cannot operate without this module."),
                Poll::Pending => (),
            }
        }

        match self.consensus.output.poll_recv(cx) {
            Poll::Ready(Some(event)) => match event {
                consensus::graph::OutEvent::GenerateSyncResponse { to, sync } => {
                    debug!("Sending sync to {}", to);
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id: to,
                        handler: NotifyHandler::Any,
                        event: protocol::Simple::GossipGraph(sync).into(),
                    });
                }
                consensus::graph::OutEvent::KnownPeersResponse(peers) => {
                    let peers = peers
                        .into_iter()
                        .enumerate()
                        .map(|(i, peer)| (peer, Sid(i.try_into().unwrap())))
                        .collect();
                    debug!(target: Targets::StorageInitialization.into_str(), "Initializing storage with distribution {:?}", peers);
                    info!("Initializing storage with distribution {:?}", peers);
                    let send_future =
                        self.consensus
                            .input
                            .send(consensus::graph::InEvent::ScheduleTx(
                                Transaction::InitializeStorage {
                                    distribution: peers,
                                },
                            ));
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("consensus.input", "ScheduleTx(InitializeStorage)"),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing will not notify other peers on program execution. for now fail fast to see this."),
                    }
                }
                consensus::graph::OutEvent::FinalizedTransaction {
                    from,
                    tx,
                    event_hash,
                } => {
                    // handle tx's:
                    // track data locations, pull assigned shards
                    match tx {
                        Transaction::StorageRequest { data_id } => {
                            debug!(
                                target: Targets::DataDistribution.into_str(),
                                "Recognized finalized storage request for {:?}", data_id
                            );
                            let event = data_memory::InEvent::StorageRequestTx(data_id, from);
                            let send_future = self.data_memory.input.send(event.clone());
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => channel_log_send!("data_memory.input", format!("{:?}", event)),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will lose track of stored shards."),
                            }
                        }
                        // take a note that `(data_id, shard_id)` is stored at `location`
                        Transaction::Stored(data_id, shard_id) => {
                            debug!(
                                target: Targets::DataDistribution.into_str(),
                                "Recognized finalized storage confirmation for {:?} by {:?}", data_id, from
                            );
                            let event = data_memory::InEvent::StoreConfirmed {
                                full_shard_id: (data_id, shard_id),
                                location: from,
                            };
                            let send_future = self.data_memory.input.send(event.clone());
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => channel_log_send!("data_memory.input", format!("{:?}", event)),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will lose track of stored shards."),
                            }
                        }
                        Transaction::Execute(instructions) => {
                            let program = match Program::new(instructions, event_hash.into()) {
                                Ok(p) => p,
                                Err(e) => cant_operate_error_return!(
                                    "could not compute hash of a program: {}",
                                    e
                                ),
                            };
                            let identifier = program.identifier().clone();
                            let send_future = self
                                .instruction_memory
                                .input
                                .send(instruction_storage::InEvent::FinalizedProgram(program));
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => channel_log_send!(
                                    "instruction_memory.input",
                                    format!("FinalizedProgram(hash: {:?})", identifier)
                                ),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!(
                                    "other half of `instruction_memory.input` was closed. \
                                        cannot operate without this module."
                                ),
                                Poll::Pending => cant_operate_error_return!(
                                    "`instruction_memory.input` queue is full. \
                                    continue will skip a transaction, which is unacceptable."
                                ),
                            }
                        }
                        Transaction::Executed(program_id) => {
                            let event = instruction_storage::InEvent::ExecutedProgram {
                                peer: from,
                                program_id,
                            };
                            let send_future = self.instruction_memory.input.send(event.clone());
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => channel_log_send!(
                                    "instruction_memory.input",
                                    format!("{:?}", event)
                                ),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!(
                                    "other half of `instruction_memory.input` was closed. \
                                        cannot operate without this module."
                                ),
                                Poll::Pending => cant_operate_error_return!(
                                    "`instruction_memory.input` queue is full. continue will \
                                    mess with confirmation of program execution, which is \
                                    unacceptable."
                                ),
                            }
                        }
                        Transaction::InitializeStorage { distribution } => {
                            debug!(
                                target: Targets::StorageInitialization.into_str(),
                                "Recognized finalized init transaction"
                            );
                            let send_future = self
                                .data_memory
                                .input
                                .send(data_memory::InEvent::Initialize { distribution });
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => {
                                    channel_log_send!("data_memory.input", "Initialize")
                                }
                                Poll::Ready(Err(_e)) => cant_operate_error_return!(
                                    "other half of `data_memory.input` was closed. \
                                        cannot operate without this module."
                                ),
                                Poll::Pending => cant_operate_error_return!(
                                    "`data_memory.input` queue is full. continuing will \
                                    lose track of stored shards."
                                ),
                            }
                        }
                    }
                }
            },
            Poll::Ready(None) => cant_operate_error_return!(
                "other half of `consensus.output` was closed. cannot operate without this module."
            ),
            Poll::Pending => (),
        }

        trace!("Checking periodic gossip");
        if self.consensus.accepts_input() {
            if let Poll::Ready(_) = self.consensus_gossip_timer.as_mut().poll(cx) {
                let random_peer = self.get_random_peer();

                // Since we're on it - make a standalone event
                let send_future = self
                    .consensus
                    .input
                    .send(consensus::graph::InEvent::CreateStandalone);
                pin_mut!(send_future);
                match send_future.poll(cx) {
                    Poll::Ready(Ok(_)) => channel_log_send!("consensus.input", "CreateStandalone"),
                    Poll::Ready(Err(_e)) => cant_operate_error_return!(
                        "other half of `consensus.input` was closed. cannot operate without this module."
                    ),
                    Poll::Pending => warn!(
                        "`consensus.input` queue is full. skipping making a standalone event. \
                        might lead to higher latency in scheduled tx inclusion."
                    ),
                };

                // Time to send another one
                self.consensus_gossip_timer = Box::pin(sleep(self.consensus_gossip_timeout));
                if let Some(random_peer) = random_peer {
                    let event = consensus::graph::InEvent::GenerateSyncRequest { to: random_peer };
                    let send_future = self.consensus.input.send(event.clone());
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => channel_log_send!("consensus.input", format!("{:?}", event)),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!(
                            "other half of `consensus.input` was closed. cannot operate without this module."
                        ),
                        Poll::Pending => warn!(
                            "`consensus.input` queue is full. skipping random gossip. \
                            it's ok for a few times, but repeated skips are concerning, \
                            as it is likely to worsen distributed system responsiveness."
                        ),
                    }
                } else {
                    warn!("Time to send gossip but no peers found, idling...");
                }
            }
        }

        match self.request_response.output.poll_recv(cx) {
            Poll::Ready(Some(e)) => {
                match e {
                    crate::request_response::OutEvent::AssignedRequestId { request_id, request } => {
                        self.pending_response.insert(request_id, request);
                    },
                    crate::request_response::OutEvent::Response { request_id, response } => {
                        match self.pending_response.get(&request_id) {
                            Some(request) => match (request, response) {
                                (
                                    protocol::Request::GetShard(full_shard_id),
                                    protocol::Response::GetShard(shard),
                                ) => {
                                    channel_log_recv!(
                                        "network.response",
                                        format!(
                                            "GetShard({:?}, is_some: {:?})",
                                            &full_shard_id,
                                            shard.is_some()
                                        )
                                    );
                                    match shard {
                                        Some(shard) => {
                                            let send_future = self.data_memory.input.send(
                                                data_memory::InEvent::AssignedResponse { full_shard_id: full_shard_id.clone(), shard }
                                            );
                                            pin_mut!(send_future);
                                            match send_future.poll(cx) {
                                                Poll::Ready(Ok(_)) => channel_log_send!("data_memory.input", format!("AssignedResponse({:?},_)", full_shard_id)),
                                                Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                                Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will discard shard served, which is not cool (?). at least it is in development."),
                                            }
                                        },
                                        None => warn!("Peer that announced that it stores assigned shard doesn't have it. Misbehaviour??"),
                                    }
                                }
                                (
                                    protocol::Request::ServeShard(full_shard_id),
                                    protocol::Response::ServeShard(shard),
                                ) => {
                                    debug!(
                                        target: Targets::DataDistribution.into_str(),
                                        "Received served shard {:?}", full_shard_id
                                    );
                                    channel_log_recv!(
                                        "network.response",
                                        format!("ServeShard({:?})", &full_shard_id)
                                    );
                                    let send_future = self.data_memory.input.send(
                                        data_memory::InEvent::ServeShardResponse(
                                            full_shard_id.clone(),
                                            shard,
                                        ),
                                    );
                                    pin_mut!(send_future);
                                    match send_future.poll(cx) {
                                        Poll::Ready(Ok(_)) => channel_log_send!("data_memory.input", format!("ServeShardResponse({:?},_)", full_shard_id)),
                                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will discard shard served, which is not cool (?). at least it is in development."),
                                    };
                                }
                                (request, response) => {
                                    warn!("Response does not match request (id {})", request_id);
                                    trace!("request: {:?}, response: {:?}", request, response);
                                }
                            },
                            None => warn!(
                                "Received response for unknown (or already fulfilled) request (id {})",
                                request_id
                            ),
                        }
                    },
                    crate::request_response::OutEvent::IncomingRequest {
                        request_id, request, channel
                    } => {
                        match request.clone() {
                            protocol::Request::GetShard((data_id, shard_id)) => {
                                let event =
                                    data_memory::InEvent::AssignedRequest((data_id, shard_id));
                                let send_future = self.data_memory.input.send(event.clone());
                                pin_mut!(send_future);
                                match send_future.poll(cx) {
                                        Poll::Ready(Ok(_)) => channel_log_send!("data_memory.input", format!("{:?}", event)),
                                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will ignore some peer's request, which is unacceptable (?)."),
                                    }
                            }
                            protocol::Request::ServeShard((data_id, shard_id)) => {
                                debug!(
                                    target: Targets::DataDistribution.into_str(),
                                    "Received request for serving shard {:?}", (&data_id, &shard_id)
                                );
                                let event =
                                    data_memory::InEvent::ServeShardRequest((data_id, shard_id));
                                let send_future = self.data_memory.input.send(event.clone());
                                pin_mut!(send_future);
                                match send_future.poll(cx) {
                                        Poll::Ready(Ok(_)) => channel_log_send!("data_memory.input", format!("{:?}", event)),
                                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will ignore some peer's request, which is unacceptable (?)."),
                                    }
                            }
                        }
                        channel_log_recv!("network.request", format!("{:?}", &request));
                        let response_handlers = self.processed_requests.entry(request).or_default();
                        response_handlers.push((request_id, channel));
                    },
                }
            },
            Poll::Ready(None) => cant_operate_error_return!("other half of `instruction_memory.output` was closed. cannot operate without this module."),
            Poll::Pending => (),
        }

        Poll::Pending
    }
}
