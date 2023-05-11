//! TODO: check accepts_input()
use std::{
    collections::{HashMap, HashSet, VecDeque},
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

use futures::{pin_mut, Future};
use libp2p::{
    swarm::{
        derive_prelude::ConnectionEstablished, ConnectionClosed, FromSwarm, NetworkBehaviour,
        NotifyHandler, ToSwarm,
    },
    PeerId,
};
use rand::Rng;

use thiserror::Error;
use tokio::{
    sync::{mpsc, Notify},
    time::{sleep, Sleep},
};
use tracing::{debug, error, trace, warn};

use crate::{
    consensus::{self, Transaction},
    data_memory::distributed_simple,
    handler::{self, Connection, ConnectionReceived},
    instruction_storage,
    processor::{
        single_threaded::{self},
        Program,
    },
    protocol, Module, State,
};

pub type OutEvent = Result<Event, Error>;

pub enum Event {}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Cannot continue behaviour operation. Shutdown (and fresh start?) is the most desirable outcome.")]
    UnableToOperate,
}

mod module {
    use std::collections::HashMap;

    use libp2p::PeerId;

    use crate::{
        processor::Instructions,
        types::{Data, Sid, Vid},
    };

    pub struct Module;

    impl crate::Module for Module {
        type InEvent = InEvent;
        type OutEvent = OutEvent;
        type SharedState = ();
    }

    pub enum InEvent {
        // schedule program, collect data, distribute data
        ScheduleProgram(Instructions),
        Get(Vid),
        Put(Vid, Data),
        ListStored,
    }

    pub enum OutEvent {
        // TODO: add hash?
        ScheduleOk,
        GetResponse(Result<(Vid, Data), crate::data_memory::distributed_simple::RecollectionError>),
        PutConfirmed(Vid),
        ListStoredResponse(Vec<(Vid, HashMap<Sid, PeerId>)>),
    }
}

struct ConnectionEvent {
    peer_id: libp2p::PeerId,
    connection: libp2p::swarm::ConnectionId,
    event: crate::handler::ConnectionReceived,
}

struct ConnectionError {
    peer_id: libp2p::PeerId,
    connection: libp2p::swarm::ConnectionId,
    error: crate::handler::ConnectionError,
}

pub struct ModuleChannelServer<M: Module> {
    pub input: mpsc::Receiver<M::InEvent>,
    pub output: mpsc::Sender<M::OutEvent>,
    state: Option<Arc<Mutex<M::SharedState>>>,
}

/// Created with [`ModuleChannelServer::new()`]
pub struct ModuleChannelClient<M: Module> {
    input: mpsc::Sender<M::InEvent>,
    output: mpsc::Receiver<M::OutEvent>,
    state: Option<Arc<Mutex<M::SharedState>>>,
}

impl<M> ModuleChannelServer<M>
where
    M: Module,
{
    pub fn new(
        initial_state: Option<M::SharedState>,
        buffer: usize,
    ) -> (ModuleChannelServer<M>, ModuleChannelClient<M>) {
        let (input_send, input_recv) = mpsc::channel(buffer);
        let (output_send, output_recv) = mpsc::channel(buffer);
        let state_shared = initial_state.map(|init| Arc::new(Mutex::new(init)));
        let server = ModuleChannelServer {
            input: input_recv,
            output: output_send,
            state: state_shared.clone(),
        };
        let client = ModuleChannelClient {
            input: input_send,
            output: output_recv,
            state: state_shared,
        };
        (server, client)
    }
}

impl<M: Module> ModuleChannelClient<M> {
    fn accepts_input(&self) -> bool {
        let Some(state) = &self.state else {
            return true
        };
        let Ok(state) = state.try_lock() else {
            return false;
        };
        state.accepts_input()
    }
}

struct Behaviour {
    local_peer_id: PeerId,

    user_interaction: ModuleChannelServer<module::Module>,
    // connections to other system components (run as separate async tasks)
    // todo: do some wrapper that'll check for timeouts and stuff. maybe also match request-response
    consensus: ModuleChannelClient<consensus::graph::Module>,
    instruction_memory: ModuleChannelClient<instruction_storage::Module>,
    data_memory: ModuleChannelClient<distributed_simple::Module>,
    processor: ModuleChannelClient<single_threaded::Module>,

    // random gossip
    connected_peers: HashSet<PeerId>,
    rng: rand::rngs::ThreadRng,
    consensus_gossip_timer: Pin<Box<Sleep>>,
    consensus_gossip_timeout: Duration,

    // connection stuff
    connection_events: VecDeque<ConnectionEvent>,
    connection_errors: VecDeque<ConnectionError>,

    currently_processed_requests: HashMap<protocol::Request, Vec<PeerId>>,
    to_notify: Vec<
        libp2p::swarm::ToSwarm<
            <Self as NetworkBehaviour>::OutEvent,
            libp2p::swarm::THandlerInEvent<Self>,
        >,
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
        data_memory: ModuleChannelClient<distributed_simple::Module>,
        processor: ModuleChannelClient<single_threaded::Module>,
    ) -> Self {
        Self {
            local_peer_id,
            user_interaction,
            consensus,
            instruction_memory,
            data_memory,
            processor,
            connected_peers: HashSet::new(),
            rng: rand::thread_rng(),
            consensus_gossip_timer: Box::pin(sleep(consensus_gossip_timeout)),
            consensus_gossip_timeout,
            connection_events: VecDeque::new(),
            connection_errors: VecDeque::new(),
            currently_processed_requests: HashMap::new(),
            to_notify: Vec::new(),
            state_updated: Arc::new(Notify::new()),
        }
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
    type ConnectionHandler = Connection;
    type OutEvent = OutEvent;

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
        match event {
            Ok(event) => self.connection_events.push_front(ConnectionEvent {
                peer_id,
                connection,
                event,
            }),
            Err(error) => self.connection_errors.push_front(ConnectionError {
                peer_id,
                connection,
                error,
            }),
        }
        self.state_updated.notify_one();
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        params: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<libp2p::swarm::ToSwarm<Self::OutEvent, libp2p::swarm::THandlerInEvent<Self>>>
    {
        // todo: reconsider ordering
        loop {
            let state_updated_notification = self.state_updated.notified();
            pin_mut!(state_updated_notification);
            // Maybe break on Pending?
            let _ = state_updated_notification.poll(cx);

            match self.connection_events.pop_back() {
                // serve shard, recieve shard, recieve gossip,
                Some(s) => {
                    match s.event {
                        ConnectionReceived::Request(request) => {
                            match request.clone() {
                                protocol::Request::ServeShard((data_id, shard_id)) => {
                                    let send_future = self.data_memory.input.send(
                                        distributed_simple::InEvent::ServeShardRequest((
                                            data_id, shard_id,
                                        )),
                                    );
                                    pin_mut!(send_future);
                                    match send_future.poll(cx) {
                                        Poll::Ready(Ok(_)) => (),
                                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will ignore some peer's request, which is unacceptable (?)."),
                                    }
                                }
                                protocol::Request::GetShard((data_id, shard_id)) => {
                                    let send_future = self.data_memory.input.send(
                                        distributed_simple::InEvent::AssignedRequest((
                                            data_id, shard_id,
                                        )),
                                    );
                                    pin_mut!(send_future);
                                    match send_future.poll(cx) {
                                        Poll::Ready(Ok(_)) => (),
                                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will ignore some peer's request, which is unacceptable (?)."),
                                    }
                                }
                            }
                            let peers_waiting = self
                                .currently_processed_requests
                                .entry(request)
                                .or_default();
                            peers_waiting.push(s.peer_id);
                        }
                        ConnectionReceived::Response(protocol::Request::ServeShard(full_shard_id), protocol::Response::ServeShard(shard)) => {
                            let send_future = self.data_memory.input.send(
                                distributed_simple::InEvent::ServeShardResponse(full_shard_id, shard)
                            );
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => (),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will discard shard served, which is not cool (?). at least it is in development."),
                            };
                        },
                        ConnectionReceived::Response(protocol::Request::GetShard(full_shard_id), protocol::Response::GetShard(shard)) => {
                            match shard {
                                Some(shard) => {
                                    let send_future = self.data_memory.input.send(
                                        distributed_simple::InEvent::AssignedResponse { full_shard_id, shard: shard }
                                    );
                                    pin_mut!(send_future);
                                    match send_future.poll(cx) {
                                        Poll::Ready(Ok(_)) => (),
                                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will discard shard served, which is not cool (?). at least it is in development."),
                                    }
                                },
                                None => warn!("Peer that announced that it stores assigned shard doesn't have it. Misbehaviour??"),
                            }
                        },
                        ConnectionReceived::Response( _, _ ) => warn!("Unmatched response, need to recheck what to do in this case."),
                        ConnectionReceived::Simple(protocol::Simple::GossipGraph(sync)) => {
                            let send_future = self.consensus.input.send(
                                consensus::graph::InEvent::ApplySync { from: s.peer_id, sync }
                            );
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => (),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing will apply received sync. for now fail fast to see this."),
                            }
                        },
                    }
                    continue;
                }
                None => (),
            }

            match self.connection_errors.pop_back() {
                // serve shard, recieve shard, recieve gossip,
                Some(e) => {
                    match e.error {
                        handler::ConnectionError::PeerUnsupported => {
                            return Poll::Ready(ToSwarm::CloseConnection {
                                peer_id: e.peer_id,
                                connection: libp2p::swarm::CloseConnection::One(e.connection),
                            })
                        }
                        // save stats mb
                        // logged in handler already; also counted there to close conneciton
                        // on too many errors
                        handler::ConnectionError::Timeout => {}
                        // Fail fast
                        handler::ConnectionError::Other(err) => {
                            // Fail fast
                            error!("Connection to {} returned error {:?}", e.peer_id, err);
                            return Poll::Ready(ToSwarm::CloseConnection {
                                peer_id: e.peer_id,
                                connection: libp2p::swarm::CloseConnection::One(e.connection),
                            });
                        }
                    }
                    continue;
                }

                None => (),
            }

            match self.to_notify.pop() {
                Some(t) => return Poll::Ready(t),
                None => (),
            }
            break;
        }

        match self.data_memory.output.poll_recv(cx) {
            Poll::Ready(Some(event)) => match event {
                distributed_simple::OutEvent::ServeShardRequest(full_shard_id, location) => {
                    // todo: separate workflow for `from` == `local_peer_id`
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id: location,
                        handler: NotifyHandler::Any,
                        event: handler::IncomingEvent::SendPrimary(
                            protocol::Primary::Request(protocol::Request::ServeShard(full_shard_id)),
                        ),
                    });
                },
                distributed_simple::OutEvent::ServeShardResponse(full_shard_id, shard) => {
                    let waiting_peers = self.currently_processed_requests.remove(
                        &protocol::Request::ServeShard(full_shard_id)
                    ).unwrap_or_default();
                    let mut new_notifications = waiting_peers.into_iter()
                        .map(|peer_id| ToSwarm::NotifyHandler {
                            peer_id,
                            handler: NotifyHandler::Any,
                            event: handler::IncomingEvent::SendResponse(protocol::Response::ServeShard(shard))
                        });
                    if let Some(next_notification) = new_notifications.next() {
                        self.to_notify.extend(new_notifications);
                        return Poll::Ready(next_notification);
                    };
                },
                distributed_simple::OutEvent::AssignedStoreSuccess(full_shard_id) => {
                    let send_future = self.consensus.input.send(
                        consensus::graph::InEvent::ScheduleTx(Transaction::Stored(full_shard_id.0, full_shard_id.1))
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing will not notify other peers on storing shard. for now fail fast to see this."),
                    }
                }
                distributed_simple::OutEvent::AssignedResponse(full_shard_id, shard) => {
                    let waiting_peers = self.currently_processed_requests.remove(
                        &protocol::Request::GetShard(full_shard_id)
                    ).unwrap_or_default();
                    let mut new_notifications = waiting_peers.into_iter()
                        .map(|peer_id| ToSwarm::NotifyHandler {
                            peer_id,
                            handler: NotifyHandler::Any,
                            event: handler::IncomingEvent::SendResponse(protocol::Response::GetShard(shard))
                        });
                    if let Some(next_notification) = new_notifications.next() {
                        self.to_notify.extend(new_notifications);
                        return Poll::Ready(next_notification);
                    };
                },
                distributed_simple::OutEvent::DistributionSuccess(data_id) => {
                    let send_future = self.user_interaction.output.send(
                        module::OutEvent::PutConfirmed(data_id)
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `user_interaction.output` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`user_interaction.output` queue is full. continuing will leave user request unanswered. for now fail fast to see this."),
                    }
                },
                distributed_simple::OutEvent::ListDistributed(list) => {
                    let send_future = self.user_interaction.output.send(
                        module::OutEvent::ListStoredResponse(list)
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `user_interaction.output` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`user_interaction.output` queue is full. continuing will leave user request unanswered. for now fail fast to see this."),
                    }
                },
                distributed_simple::OutEvent::PreparedServiceResponse(data_id) => {
                    let send_future = self.consensus.input.send(
                        consensus::graph::InEvent::ScheduleTx(Transaction::StorageRequest { address: data_id })
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing might not fulfill user's expectations. for now fail fast to see this."),
                    }
                },
                distributed_simple::OutEvent::AssignedRequest(full_shard_id, location) => {
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id: location,
                        handler: NotifyHandler::Any,
                        event: handler::IncomingEvent::SendPrimary(
                            protocol::Primary::Request(protocol::Request::GetShard(
                                full_shard_id,
                            )),
                        ),
                    });
                }
                distributed_simple::OutEvent::RecollectResponse(response) => {
                    let send_future = self.user_interaction.output.send(
                        module::OutEvent::GetResponse(response)
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
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
                module::InEvent::ScheduleProgram(instructions) => {
                    let send_future = self.consensus.input.send(
                        consensus::graph::InEvent::ScheduleTx(Transaction::Execute(instructions))
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing might not fulfill user's expectations. for now fail fast to see this."),
                    }
                    let send_future = self.user_interaction.output.send(
                        module::OutEvent::ScheduleOk
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `user_interaction.output` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`user_interaction.output` queue is full. continuing will leave user request unanswered. for now fail fast to see this."),
                    }
                },
                module::InEvent::Get(data_id) => {
                    let send_future = self.data_memory.input.send(
                        distributed_simple::InEvent::RecollectRequest(data_id)
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing might not fulfill user's expectations. for now fail fast to see this."),
                    }
                },
                module::InEvent::Put(data_id, data) => {
                    let send_future = self.data_memory.input.send(
                        distributed_simple::InEvent::PrepareServiceRequest { data_id, data }
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing might not fulfill user's expectations. for now fail fast to see this."),
                    }
                },
                module::InEvent::ListStored => {
                    let send_future = self.data_memory.input.send(
                        distributed_simple::InEvent::ListDistributed
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing might not fulfill user's expectations. for now fail fast to see this."),
                    }
                }
            },
            Poll::Ready(None) => cant_operate_error_return!("`user_interaction.input` (at client) was closed. not intended to operate without interaction with user."),
            Poll::Pending => (),
        }

        match self.processor.output.poll_recv(cx) {
            Poll::Ready(Some(single_threaded::OutEvent::FinishedExecution { program_id, results })) => {
                debug!("Finished executing program {:?}\nResults: {:?}", program_id.clone(), results);
                let send_future = self.consensus.input.send(
                    consensus::graph::InEvent::ScheduleTx(Transaction::Executed(program_id))
                );
                pin_mut!(send_future);
                match send_future.poll(cx) {
                    Poll::Ready(Ok(_)) => (),
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
                                Poll::Ready(Ok(_)) => (),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `processor.input` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`processor.input` queue is full. continuing will skip a program for execution, which is unacceptable."),
                            }
                        }
                        instruction_storage::OutEvent::FinishedExecution(_) => todo!(),
                    }
                }
                Poll::Ready(None) => cant_operate_error_return!("other half of `instruction_memory.output` was closed. cannot operate without this module."),
                Poll::Pending => (),
            }
        }

        match self.consensus.output.poll_recv(cx) {
            Poll::Ready(Some(event)) => match event {
                consensus::graph::OutEvent::FinalizedTransaction {
                    from,
                    tx,
                    event_hash,
                } => {
                    // handle tx's:
                    // track data locations, pull assigned shards
                    match tx {
                        Transaction::StorageRequest { address } => {
                            let send_future = self
                                .data_memory
                                .input
                                .send(distributed_simple::InEvent::StorageRequestTx(address, from));
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => (),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will lose track of stored shards."),
                            }
                        }
                        // take a note that `(data_id, shard_id)` is stored at `location`
                        Transaction::Stored(data_id, shard_id) => {
                            let send_future = self.data_memory.input.send(
                                distributed_simple::InEvent::StoreConfirmed {
                                    full_shard_id: (data_id, shard_id),
                                    location: from,
                                },
                            );
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => (),
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
                            let send_future = self
                                .instruction_memory
                                .input
                                .send(instruction_storage::InEvent::FinalizedProgram(program));
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => (),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `instruction_memory.input` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`instruction_memory.input` queue is full. continue will skip a transaction, which is unacceptable."),
                            }
                        }
                        Transaction::Executed(program_id) => {
                            let send_future = self.instruction_memory.input.send(
                                instruction_storage::InEvent::ExecutedProgram {
                                    peer: from,
                                    program_id,
                                },
                            );
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => (),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `instruction_memory.input` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`instruction_memory.input` queue is full. continue will mess with confirmation of program execution, which is unacceptable."),
                            }
                        }
                        Transaction::InitializeStorage { distribution } => {
                            let send_future = self
                                .data_memory
                                .input
                                .send(distributed_simple::InEvent::Initialize { distribution });
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => (),
                                Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will lose track of stored shards."),
                            }
                        }
                    }
                }
                consensus::graph::OutEvent::SyncReady { to, sync } => {
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id: to,
                        handler: NotifyHandler::Any,
                        event: handler::IncomingEvent::SendPrimary(protocol::Primary::Simple(
                            protocol::Simple::GossipGraph(sync),
                        )),
                    });
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
                    Poll::Ready(Ok(_)) => (),
                    Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                    Poll::Pending => warn!("`consensus.input` queue is full. skipping making a standalone event. might lead to higher latency in scheduled tx inclusion."),
                };

                // Time to send another one
                self.consensus_gossip_timer = Box::pin(sleep(self.consensus_gossip_timeout));
                if let Some(random_peer) = random_peer {
                    let send_future = self
                        .consensus
                        .input
                        .send(consensus::graph::InEvent::GenerateSync { to: random_peer });
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(_e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                        Poll::Pending => warn!("`consensus.input` queue is full. skipping random gossip. it's ok for a few times, but repeated skips are concerning, as it is likely to worsen distributed system responsiveness."),
                    }
                } else {
                    debug!("Time to send gossip but no peers found, idling...");
                }
            }
        }

        Poll::Pending
    }
}
