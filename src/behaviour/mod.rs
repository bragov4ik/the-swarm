//! TODO: check accepts_input()
use std::{
    collections::{HashMap, HashSet, VecDeque},
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

use futures::{pin_mut, Future, Stream};
use libp2p::{
    swarm::{
        derive_prelude::ConnectionEstablished, ConnectionClosed, FromSwarm, NetworkBehaviour,
        NotifyHandler, ToSwarm,
    },
    PeerId,
};
use rand::Rng;
use rust_hashgraph::algorithm::MockSigner;
use thiserror::Error;
use tokio::{
    sync::{mpsc, Notify},
    time::{sleep, Sleep},
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    consensus::{self, Transaction},
    data_memory::{self, distributed_simple},
    handler::{self, Connection, ConnectionReceived},
    instruction_storage,
    processor::single_threaded::{self, Program},
    protocol,
    types::{Data, GraphSync, Sid, Vid},
    Module, State,
};

use self::execution::Execution;

pub type OutEvent = Result<Event, Error>;

pub enum Event {}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Cannot continue behaviour operation. Shutdown (and fresh start?) is the most desirable outcome.")]
    UnableToOperate,
}

mod module {
    use crate::{
        processor::single_threaded::Program,
        types::{Data, Vid},
    };

    pub struct Module;

    impl crate::Module for Module {
        type InEvent = InEvent;
        type OutEvent = OutEvent;
        type State = ();
    }

    pub enum InEvent {
        // schedule program, collect data, distribute data
        ScheduleProgram(Program),
        Get(Vid),
        Put(Vid, Data),
    }

    pub enum OutEvent {
        // TODO: add hash?
        ScheduleOk,
        GetResponse(Vid, Data),
        PutConfirmed(Vid),
    }
}

// serve piece, recieve piece, recieve gossip,
enum NetworkRequest {
    ServePiece,
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
    input: mpsc::Receiver<M::InEvent>,
    output: mpsc::Sender<M::OutEvent>,
    state: Option<Arc<Mutex<M::State>>>,
}

pub struct ModuleChannelClient<M: Module> {
    input: mpsc::Sender<M::InEvent>,
    output: mpsc::Receiver<M::OutEvent>,
    state: Option<Arc<Mutex<M::State>>>,
}

impl<M: Module> ModuleChannelClient<M> {
    fn accepts_input(&self) -> bool {
        let Some(state) = self.state else {
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
    // todo: do some wrapper that'll check for timeouts and stuff. maybe match request-response
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

mod execution {
    use futures::Stream;

    use crate::processor::single_threaded::Program;

    pub struct Execution {}

    impl Stream for Execution {
        // return hash of executed program???
        type Item = ();

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            todo!()
        }
    }

    impl Execution {
        pub fn initiate(&mut self, program: Program) -> Result<(), ()> {
            todo!()
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
                .expect("Shouldn't have skipped more than `len-1` elements"),
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
                    warn!("Newly connecting peer was already in connected list, data is inconsistent.");
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
                    warn!("Disconnecting peer wasn't in connected list, data is inconsistent.");
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
            state_updated_notification.poll(cx);

            match self.connection_events.pop_back() {
                // serve piece, recieve piece, recieve gossip,
                Some(s) => {
                    match s.event {
                        ConnectionReceived::Request(request) => {
                            match request {
                                protocol::Request::ServeShard((data_id, piece_id)) => {
                                    let send_future = self.data_memory.input.send(
                                        distributed_simple::InEvent::ServePiece((
                                            data_id, piece_id,
                                        )),
                                    );
                                    pin_mut!(send_future);
                                    match send_future.poll(cx) {
                                        Poll::Ready(Ok(_)) => (),
                                        Poll::Ready(Err(e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will ignore some peer's request, which is unacceptable (?)."),
                                    }
                                }
                                protocol::Request::GetShard((data_id, piece_id)) => {
                                    todo!()
                                }
                            }
                            let peers_waiting = self
                                .currently_processed_requests
                                .entry(request)
                                .or_default();
                            peers_waiting.push(s.peer_id);
                        }
                        ConnectionReceived::Response( protocol::Request::ServeShard(request), protocol::Response::ServeShard(shard)) => {
                            match shard {
                                Some(shard) => {
                                    let send_future = self.data_memory.input.send(
                                        distributed_simple::InEvent::StoreAssigned{ full_piece_id: request, data: shard }
                                    );
                                    pin_mut!(send_future);
                                    match send_future.poll(cx) {
                                        Poll::Ready(Ok(_)) => (),
                                        Poll::Ready(Err(e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will discard piece served, which is not cool (?). at least it is in development"),
                                    }
                                },
                                None => warn!("Peer that announced peer distribution doesn't have shard assigned to us. Strange but ok."),
                            }
                        },
                        ConnectionReceived::Response( protocol::Request::GetShard(request), protocol::Response::GetShard(shard)) => todo!(),
                        ConnectionReceived::Response( _, _ ) => warn!("Unmatched response, need to recheck what to do in this case"),
                        ConnectionReceived::Simple(_) => todo!(),
                    }
                    continue;
                }
                None => (),
            }

            match self.connection_errors.pop_back() {
                // serve piece, recieve piece, recieve gossip,
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
                        handler::ConnectionError::Other(err) => cant_operate_error_return!(
                            "Connection to {} returned error {:?}",
                            e.peer_id,
                            err
                        ),
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
                distributed_simple::OutEvent::ServedPiece((data_id, shard_id), shard) => {
                    let waiting_peers = self.currently_processed_requests.remove(
                        &protocol::Request::ServeShard((data_id, shard_id))
                    ).unwrap_or_default();
                    let new_notifications = waiting_peers.into_iter()
                        .map(|peer_id| ToSwarm::NotifyHandler {
                            peer_id,
                            handler: NotifyHandler::Any,
                            event: handler::IncomingEvent::SendResponse(protocol::Response::ServeShard(shard))
                        });
                    if let Some(next_notification) = new_notifications.next() {
                        self.to_notify.extend(new_notifications);
                        return   Poll::Ready(next_notification);
                    };
                },
                distributed_simple::OutEvent::AssignedStoreSuccess(full_piece_id) => {
                    let send_future = self.consensus.input.send(
                        consensus::graph::InEvent::ScheduleTx(Transaction::Stored(full_piece_id.0, full_piece_id.1))
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing will not notify other peers on. for now fail fast to see this."),
                    }
                }
                distributed_simple::OutEvent::AssignedEvent { full_piece_id, data } => todo!(),
                distributed_simple::OutEvent::PreparedForService { data_id, distribution } => {
                    let send_future = self.consensus.input.send(
                        consensus::graph::InEvent::ScheduleTx(Transaction::StorageRequest { address: data_id, distribution })
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing might not fulfill user's expectations. for now fail fast to see this."),
                    }
                },
            },
            Poll::Ready(None) => cant_operate_error_return!("other half of `data_memory.output` was closed. cannot operate without this module."),
            Poll::Pending => (),
        }

        // TODO: check if futures::select! is applicable to avoid starvation (??)
        match self.user_interaction.input.poll_recv(cx) {
            Poll::Ready(Some(event)) => match event {
                module::InEvent::ScheduleProgram(program) => {
                    let send_future = self.consensus.input.send(
                        consensus::graph::InEvent::ScheduleTx(Transaction::Execute(program))
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(e)) => cant_operate_error_return!("other half of `consensus.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`consensus.input` queue is full. continuing might not fulfill user's expectations. for now fail fast to see this."),
                    }
                },
                module::InEvent::Get(_) => todo!(),
                module::InEvent::Put(data_id, data) => {
                    let send_future = self.data_memory.input.send(
                        distributed_simple::InEvent::PrepareForService { data_id, data }
                    );
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing might not fulfill user's expectations. for now fail fast to see this."),
                    }
                },
            },
            Poll::Ready(None) => cant_operate_error_return!("`user_interaction.input` (at client) was closed. not intended to operate without interaction with user."),
            Poll::Pending => (),
        }

        match self.processor.output.poll_recv(cx) {
            Poll::Ready(Some(single_threaded::OutEvent::FinishedExecution{ results: _ })) => {
                // todo add hash?
                info!("Executed program {}", 0);
            }
            Poll::Ready(None) => cant_operate_error_return!("other half of `instruction_memory.output` was closed. cannot operate without this module."),
            Poll::Pending => (),
        }

        if self.processor.accepts_input() {
            match self.instruction_memory.output.poll_recv(cx) {
                Poll::Ready(Some(instruction_storage::OutEvent::NextProgram(program))) => {
                    let send_future = self.processor.input.send(single_threaded::InEvent::Execute(program));
                    pin_mut!(send_future);
                    match send_future.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(e)) => cant_operate_error_return!("other half of `processor.input` was closed. cannot operate without this module."),
                        Poll::Pending => cant_operate_error_return!("`processor.input` queue is full. continuing will skip a program for execution, which is unacceptable."),
                    }
                }
                Poll::Ready(None) => cant_operate_error_return!("other half of `instruction_memory.output` was closed. cannot operate without this module."),
                Poll::Pending => (),
            }
        }

        match self.consensus.output.poll_recv(cx) {
            Poll::Ready(Some(event)) => match event {
                consensus::graph::OutEvent::FinalizedTransaction { from, tx } => {
                    // handle tx's:
                    // track data locations, pull assigned shards
                    match tx {
                        Transaction::StorageRequest {
                            address,
                            distribution,
                        } => {
                            if let Some(assigned) = distribution
                                .into_iter()
                                .find(|(peer_id, _)| self.local_peer_id == *peer_id)
                            {
                                // todo: separate workflow for `from` == `local_peer_id`
                                return Poll::Ready(ToSwarm::NotifyHandler {
                                    peer_id: from,
                                    handler: NotifyHandler::Any,
                                    event: handler::IncomingEvent::SendPrimary(
                                        protocol::Primary::Request(protocol::Request::ServeShard(
                                            (address, assigned.1),
                                        )),
                                    ),
                                });
                            }
                        }
                        // take a note that `(data_id, piece_id)` is stored at `location`
                        Transaction::Stored(data_id, piece_id) => {
                            let send_future = self.data_memory.input.send(
                                distributed_simple::InEvent::TrackLocation {
                                    full_piece_id: (data_id, piece_id),
                                    location: from,
                                },
                            );
                            pin_mut!(send_future);
                            match send_future.poll(cx) {
                                Poll::Ready(Ok(_)) => (),
                                Poll::Ready(Err(e)) => cant_operate_error_return!("other half of `data_memory.input` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`data_memory.input` queue is full. continuing will lose track of stored pieces."),
                            }
                        }
                        Transaction::Execute(program) => {
                            let programs_to_run_send = self
                                .instruction_memory
                                .input
                                .send(instruction_storage::InEvent::FinalizedProgram(program));
                            pin_mut!(programs_to_run_send);
                            match programs_to_run_send.poll(cx) {
                                Poll::Ready(Ok(_)) => (),
                                Poll::Ready(Err(e)) => cant_operate_error_return!("other half of `instruction_memory.input` was closed. cannot operate without this module."),
                                Poll::Pending => cant_operate_error_return!("`instruction_memory.input` queue is full. continue will skip a transaction, which is unacceptable."),
                            }
                        }
                    }
                }
                consensus::graph::OutEvent::SyncReady { to, sync } => todo!(),
            },
            Poll::Ready(None) => cant_operate_error_return!(
                "other half of `consensus.output` was closed. cannot operate without this module."
            ),
            Poll::Pending => (),
        }

        let finalized_transactions = &mut self.finalized_transactions;
        pin_mut!(finalized_transactions);
        match finalized_transactions.poll_next(cx) {
            // handle tx's:
            // track data locations, pull assigned shards
            Poll::Ready(Some(tx)) => match tx {
                // send a network request to `who`
                (
                    who,
                    Transaction::StorageRequest {
                        address,
                        distribution,
                    },
                ) => todo!(),
                // take a note that `(data_id, piece_id)` is stored at `location`
                (location, Transaction::Stored(data_id, piece_id)) => todo!(),
                (_, Transaction::Execute(p)) => {
                    let programs_to_run_send = self.programs_to_run.sender.send(p);
                    pin_mut!(programs_to_run_send);
                    match programs_to_run_send.poll(cx) {
                        Poll::Ready(Ok(_)) => (),
                        Poll::Ready(Err(e)) => cant_operate_error_return!("other half of `programs_to_run` was closed, but it's owned by us. it's a bug."),
                        Poll::Pending => cant_operate_error_return!("`programs_to_run` queue is full, probably mistake code with tasks priority. continuing will skip a transaction, which is unacceptable."),
                    }
                }
            },
            Poll::Ready(None) => cant_operate_error_return!("other half of `finalized_transactions` was closed, but it's owned by us. it's a bug."),
            Poll::Pending => (),
        }

        trace!("Checking periodic gossip");
        if self.consensus.accepts_input() {
            if let Poll::Ready(_) = self.consensus_gossip_timer.as_mut().poll(cx) {
                // Time to send another one
                let random_peer = self.get_random_peer();
                self.consensus_gossip_timer = Box::pin(sleep(self.consensus_gossip_timeout));
                if let Some(random_peer) = random_peer {
                    debug!("Generating sync for peer {}", random_peer);
                    // self.consensus.input;
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id: random_peer,
                        handler: NotifyHandler::Any,
                        event: HandlerEvent::SendPrimary(Primary::Simple(Simple::GossipGraph(
                            self.consensus.get_sync(&random_peer),
                        ))),
                    });
                } else {
                    debug!("Time to send gossip but no peers found, idling...");
                }
            }
        }

        Poll::Pending
    }
}
