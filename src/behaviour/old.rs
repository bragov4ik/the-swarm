//! Main behaviour of the network node.
//!
//! Assumes connected peer discovery running side by side and calling
//! [Behaviour::inject_peer_discovered] [Behaviour::inject_peer_expired].
//!
//! ## Functions (approximate list):
//! - Answer incoming requests
//! - Random gossip of state (like in graph consensus, as template for future integration)
//! - Instruction execution
//! - Data discovery & requesting
//!
//! ## TODO
//! - Actual consensus integration
//! - Interactive data import
//! - Better UX
//! - (other TODOs in code)
//!
//! ## Mock implementation
//!
//! Right now implemented for demonstration. Since we don't have a
//! proper consensus right now, there is one main node (with flag `is_main`)
//! that distributes the initial data to the nodes. Then it adds
//! predefined list of instructions (which is gossiped together with mock
//! consensus state) and executes them, requesting missing data from
//! the nodes.
//!
//! The secondary nodes also execute the instructions, therefore at the end
//! initial vectors are spread randomly, and newly created ones are
//! replicated on each node.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    pin::Pin,
    task::Poll,
    time::Duration,
};

use crate::{
    consensus::{DataDiscoverer, GraphConsensus, Transaction},
    data_memory::DataMemory,
    handler::{Connection, ConnectionError, ConnectionReceived, IncomingEvent as HandlerEvent},
    instruction_storage::InstructionMemory,
    processor::{Instruction, Operation, Processor, UnaryOp},
    protocol::{Primary, Request, Response, Simple},
    types::{Shard, Vid},
};
use futures::Future;
use libp2p::{
    swarm::{
        derive_prelude::ConnectionEstablished,
        dial_opts::{DialOpts, PeerCondition},
        ConnectionClosed, NetworkBehaviour, NotifyHandler, THandlerInEvent, ToSwarm,
    },
    PeerId,
};
use rand::Rng;
use tokio::time::{sleep, Sleep};
use tracing::{debug, error, info, trace, warn};

struct ConnectionEvent {
    peer_id: libp2p::PeerId,
    connection: libp2p::swarm::ConnectionId,
    event: Result<ConnectionReceived, ConnectionError>,
}

pub struct Behaviour<TConsensus, TDataMemory, TProcessor>
where
    TDataMemory: DataMemory,
{
    consensus: TConsensus,
    data_memory: TDataMemory,
    _processor: TProcessor,

    local_peer_id: PeerId,
    discovered_peers: VecDeque<PeerId>,

    /// Random gossip
    connected_peers: HashSet<PeerId>,
    rng: rand::rngs::ThreadRng,
    consensus_gossip_timer: Pin<Box<Sleep>>,
    consensus_gossip_timeout: Duration,

    connection_events: VecDeque<ConnectionEvent>,
    // TODO: timeout, ensure uniqueness/validity?
    incoming_shards_buffer: HashMap<Vid, VecDeque<Shard>>,

    // Temporary fields needed so that mock implementation would
    // work (all below). TODO: remove/replace
    is_main_node: bool,
    data_to_distribute: VecDeque<(Vid, Shard)>,
    distribute: bool,
    instructions_to_execute: VecDeque<Instruction<Vid, Vid>>,
    execute: bool,

    /// Execution status (to be removed/completely changed with
    /// actual consensus, it's a mock part)
    exec_state: ExecutionState<TDataMemory::Identifier, TDataMemory::Piece>,
    pending_handler_events: VecDeque<(PeerId, HandlerEvent)>,
}

pub trait DataMemoryReadAll<I, D> {
    fn read_all(&self) -> Vec<(I, D)>;
}

// TODO: remove, demo
impl<C, D, P> Behaviour<C, D, P>
where
    D: DataMemory + DataMemoryReadAll<Vid, Shard>,
{
    pub fn read_all_local(&self) -> Vec<(Vid, Shard)> {
        self.data_memory.read_all()
    }
}

enum ExecutionState<TDataId, TData> {
    WaitingData {
        instruction: Instruction<(TDataId, Option<TData>), TDataId>,
    },
    WaitingInstruction,
}

#[derive(Debug)]
pub enum MockInitError {
    NotMainNode,
}

// TODO: remove, temp stuff for mock
impl<C, D, P> Behaviour<C, D, P>
where
    D: DataMemory<Identifier = Vid>,
    C: InstructionMemory<Instruction = Instruction<D::Identifier, D::Identifier>>,
{
    pub fn add_data_to_distribute(&mut self, id: Vid, data: Shard) -> Result<(), MockInitError> {
        if self.is_main_node {
            self.data_to_distribute.push_front((id, data));
            Ok(())
        } else {
            Err(MockInitError::NotMainNode)
        }
    }

    pub fn add_instruction(
        &mut self,
        instruction: Instruction<D::Identifier, D::Identifier>,
    ) -> Result<(), MockInitError> {
        if self.is_main_node {
            self.instructions_to_execute.push_front(instruction);
            Ok(())
        } else {
            Err(MockInitError::NotMainNode)
        }
    }

    pub fn allow_distribution(&mut self) {
        self.distribute = true;
    }

    pub fn allow_execution(&mut self) {
        self.execute = true;
    }
}

impl<C, D: DataMemory, P> Behaviour<C, D, P> {
    pub fn new(
        consensus: C,
        data_memory: D,
        _processor: P,
        consensus_gossip_timeout: Duration,
        is_main_node: bool,
        local_peer_id: PeerId,
    ) -> Self {
        Self {
            consensus,
            data_memory,
            _processor,
            local_peer_id,
            discovered_peers: VecDeque::new(),
            connected_peers: HashSet::new(),
            rng: rand::thread_rng(),
            consensus_gossip_timer: Box::pin(sleep(consensus_gossip_timeout)),
            consensus_gossip_timeout,
            connection_events: VecDeque::new(),
            incoming_shards_buffer: HashMap::new(),
            is_main_node,
            data_to_distribute: VecDeque::new(),
            distribute: false,
            instructions_to_execute: VecDeque::new(),
            execute: false,
            exec_state: ExecutionState::WaitingInstruction,
            pending_handler_events: VecDeque::new(),
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

    /// Update given entry if it's empty and shard (data) for corresponding id has arrived
    fn retrieve_from_buf(
        buf: &mut HashMap<Vid, VecDeque<Shard>>,
        id: &Vid,
        entry: &mut Option<Shard>,
    ) {
        if entry.is_some() {
            return;
        }
        let queue = match buf.get_mut(id) {
            Some(b) => b,
            None => return,
        };
        if let Some(shard) = queue.pop_back() {
            *entry = Some(shard);
            if queue.is_empty() {
                buf.remove(id);
            }
        }
    }
}

impl<C, D, P> Behaviour<C, D, P>
where
    D: DataMemory,
    C: DataDiscoverer<DataIdentifier = Vid, PeerAddr = PeerId>,
{
    fn place_data_request(&mut self, id: Vid) -> Result<(), NoPeerFound> {
        let locations = self.consensus.shard_locations(&id);
        if locations.is_empty() {
            return Err(NoPeerFound(id));
        }
        for loc in locations {
            self.pending_handler_events.push_front((
                loc,
                HandlerEvent::SendPrimary(Primary::Request(Request::Shard(id.clone()))),
            ));
        }
        Ok(())
    }
}
impl<C, D, P> Behaviour<C, D, P>
where
    C: GraphConsensus<OperandId = Vid, PeerId = PeerId>,
    D: DataMemory<Identifier = Vid>,
    D::Identifier: Clone,
{
    /// Save in memory and register the fact in consensus.
    fn save_shard_locally(
        &mut self,
        id: D::Identifier,
        shard_id: C::OperandPieceId,
        shard: D::Piece,
        local_id: PeerId,
    ) {
        if let Err(e) = self.data_memory.store_piece(id.clone(), shard) {
            warn!("Error saving shard locally: {:?}", e);
        } else if let Err(e) = self.consensus.push_tx(Transaction::Stored(id, shard_id)) {
            warn!(
                "Error announcing saving shard (may lead to \"dangling\" shard in local mem): {:?}",
                e
            );
        }
        debug!("Saved shard locally.");
    }
}

/// No peer found for vector `Vid`
#[derive(Debug)]
struct NoPeerFound(Vid);

impl<TConsensus, TDataMemory, TProcessor> NetworkBehaviour
    for Behaviour<TConsensus, TDataMemory, TProcessor>
where
    // Operator = Vid because we don't store actual data in the consensus
    TConsensus: GraphConsensus<
            SyncPayload = crate::types::Graph,
            OperandId = Vid,
            PeerId = PeerId,
            OperandPieceId = (),
        > + DataDiscoverer<DataIdentifier = <TDataMemory as DataMemory>::Identifier, PeerAddr = PeerId>
        + 'static,
    TDataMemory: DataMemory<Identifier = Vid, Piece = Shard> + 'static,
    TProcessor: Processor<Operand = Shard> + 'static,
{
    type ConnectionHandler = Connection;
    type OutEvent = ();

    // TODO: check https://github.com/libp2p/rust-libp2p/blob/master/swarm/CHANGELOG.md#0420
    // maybe do separate behaviour for managing connections

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: libp2p::swarm::ConnectionId,
        _peer: PeerId,
        _local_addr: &libp2p::Multiaddr,
        _remote_addr: &libp2p::Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        debug!("Creating new inbound connection handler");
        Ok(Connection::new(10))
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: libp2p::swarm::ConnectionId,
        _peer: PeerId,
        _addr: &libp2p::Multiaddr,
        _role_override: libp2p::core::Endpoint,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        debug!("Creating new out bound connection handler");
        Ok(Connection::new(10))
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        _params: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<ToSwarm<Self::OutEvent, THandlerInEvent<Self>>> {
        // Maybe later split request handling, gossiping, processing into different behaviours

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

        trace!("Checking pending handler events to send");
        match self.pending_handler_events.pop_back() {
            Some((addr, e)) => {
                return Poll::Ready(ToSwarm::NotifyHandler {
                    peer_id: addr,
                    handler: NotifyHandler::Any,
                    event: e,
                })
            }
            None => {}
        }

        // Basically handling incoming requests & responses
        trace!("Checking events from peer connections");
        match self.connection_events.pop_back() {
            Some(ConnectionEvent {
                peer_id,
                connection,
                event,
            }) => {
                match event {
                    Ok(success) => match success {
                        ConnectionReceived::Request(Request::Shard(id)) => {
                            debug!(
                                "Received request for getting vector {:?} shard, responding",
                                id
                            );
                            let result = self.data_memory.get_piece(&id).cloned();
                            return Poll::Ready(ToSwarm::NotifyHandler {
                                peer_id,
                                handler: NotifyHandler::One(connection),
                                event: HandlerEvent::SendResponse(Response::Shard(result)),
                            });
                        }
                        ConnectionReceived::Response(
                            Request::Shard(id),
                            Response::Shard(shard),
                        ) => match shard {
                            Some(shard) => {
                                debug!("Received shard for vector {:?}", id);
                                if let Some(queue) = self.incoming_shards_buffer.get_mut(&id) {
                                    queue.push_front(shard)
                                } else {
                                    self.incoming_shards_buffer
                                        .insert(id, VecDeque::from(vec![shard]));
                                }
                            }
                            None => {
                                debug!("Received response for vector {:?} but without shard", id)
                            }
                        },
                        ConnectionReceived::Simple(Simple::GossipGraph(graph)) => {
                            debug!("Received graph update");
                            match self.consensus.update_graph(graph) {
                                Ok(()) => {}
                                Err(err) => warn!("Error updating graph with gossip: {:?}", err),
                            }
                        }
                        ConnectionReceived::Simple(Simple::StoreShard((id, data))) => {
                            debug!("Received request to save shard of data id {:?}", id);
                            self.save_shard_locally(id, (), data, self.local_peer_id);
                        }
                    },
                    Err(ConnectionError::PeerUnsupported) => {
                        return Poll::Ready(ToSwarm::CloseConnection {
                            peer_id,
                            connection: libp2p::swarm::CloseConnection::One(connection),
                        })
                    }
                    // save stats mb
                    // logged in handler already; also counted there to close conneciton
                    // on too many errors
                    Err(ConnectionError::Timeout) => {}
                    Err(ConnectionError::Other(err)) => {
                        // Fail fast
                        error!("Connection to {} returned error {:?}", peer_id, err);
                        return Poll::Ready(ToSwarm::CloseConnection {
                            peer_id,
                            connection: libp2p::swarm::CloseConnection::One(connection),
                        });
                    }
                }
            }
            None => {}
        }

        trace!("Checking periodic gossip");
        match self.consensus_gossip_timer.as_mut().poll(cx) {
            Poll::Ready(_) => {
                // Time to send another one
                let random_peer = self.get_random_peer();
                self.consensus_gossip_timer = Box::pin(sleep(self.consensus_gossip_timeout));
                if let Some(random_peer) = random_peer {
                    debug!("Sending gossip to peer {}", random_peer);
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
            Poll::Pending => {
                // Just wait
            }
        }

        trace!("Distributing received shards to \"waiters\"");
        match &mut self.exec_state {
            ExecutionState::WaitingInstruction => {}
            ExecutionState::WaitingData { instruction } => {
                let buf = &mut self.incoming_shards_buffer;
                match instruction.operation {
                    Operation::Dot(operation) | Operation::Plus(operation) => {
                        Self::retrieve_from_buf(buf, &operation.first.0, &mut operation.first.1);
                        Self::retrieve_from_buf(buf, &operation.second.0, &mut operation.second.1);
                    }
                    Operation::Inv(UnaryOp { operand }) => {
                        Self::retrieve_from_buf(buf, &operand.0, &mut operand.1)
                    }
                }
            }
        }

        trace!("Checking computations scheduled");
        match &self.exec_state {
            ExecutionState::WaitingData { instruction } => {
                // `Some(<instruction>)` if all operands are retrieved and we're ready to execute it
                let ready_instruction = instruction
                    .as_ref()
                    .map_operands(|o| o.1.as_ref())
                    .transpose_operation();
                // TODO: remove print of whole instruction
                if let Some(ready_instruction) = ready_instruction {
                    debug!(
                        "Received all data, executing instruction {:?}",
                        ready_instruction
                    );
                    match <TProcessor as Processor>::execute_one(&ready_instruction) {
                        Ok(res) => {
                            let dest_id = (*ready_instruction.result).clone();
                            if self.data_memory.get_piece(&dest_id).is_some() {
                                warn!("Tried to overwrite data in instruction, the execution result is not saved")
                            } else {
                                match self.data_memory.store_piece(dest_id, res) {
                                    Ok(None) => debug!("Executed and saved result."),
                                    // Shouldn't happen, we've just checked it
                                    Ok(Some(_)) => error!("Overwrote data after executing an instruction. This behaviour is unintended and is most likely a bug."),
                                    Err(e) => error!("Error saving result: {:?}", e),
                                }
                            }
                        }
                        Err(e) => error!("Error executing instruction: {:?}", e),
                    }
                    // Updating state
                    self.exec_state = ExecutionState::WaitingInstruction;
                }
            }
            ExecutionState::WaitingInstruction => {
                if let Some(instruction) = self.consensus.next_instruction() {
                    debug!("Found new instruction to execute: {:?}", instruction);
                    // Now we need to obtain data for computations. We try to get it from local storage,
                    // if unsuccessful, discover & send requests to corresponding nodes.
                    let state_instruction = match instruction {
                        Instruction::dot(i1, i2, dest) => Instruction::dot(
                            (i1.clone(), self.data_memory.get_piece(&i1).cloned()),
                            (i2.clone(), self.data_memory.get_piece(&i2).cloned()),
                            dest,
                        ),
                        Instruction::plus(i1, i2, dest) => Instruction::plus(
                            (i1.clone(), self.data_memory.get_piece(&i1).cloned()),
                            (i2.clone(), self.data_memory.get_piece(&i2).cloned()),
                            dest,
                        ),
                        Instruction::inv(i, dest) => Instruction::inv(
                            (i.clone(), self.data_memory.get_piece(&i).cloned()),
                            dest,
                        ),
                    };

                    debug!("Scheduling data requests (if needed)");
                    // Schedule data requests, if needed
                    let success = match &state_instruction {
                        Instruction::dot((i1, opt1), (i2, opt2), _)
                        | Instruction::plus((i1, opt1), (i2, opt2), _) => {
                            let res1 = if opt1.is_none() {
                                self.place_data_request(i1.clone())
                            } else {
                                Ok(())
                            };
                            let res2 = if opt2.is_none() {
                                self.place_data_request(i2.clone())
                            } else {
                                Ok(())
                            };
                            res1.and(res2)
                        }
                        Instruction::inv((i, opt), _) => {
                            if opt.is_none() {
                                self.place_data_request(i.clone())
                            } else {
                                Ok(())
                            }
                        }
                    };

                    // Updating state, if needed
                    match success {
                        Ok(_) => {
                            self.exec_state = ExecutionState::WaitingData {
                                instruction: state_instruction,
                            }
                        }
                        Err(e) => warn!(
                            "Could not find peer id that stores vector {:?}, skipping instruction",
                            e
                        ),
                    }
                }
            }
        }

        // TODO: remove (mock)
        trace!("Checking if allowed to execute by user");
        if self.execute {
            while let Some(instruction) = self.instructions_to_execute.pop_back() {
                if let Err(e) = self.consensus.push_instruction(instruction.clone()) {
                    warn!("Error queueing next instruction: {:?}", e);
                } else {
                    debug!("Added instruction {:?}", instruction);
                }
            }
            info!("Finished adding initial instructions");
            self.execute = false;
        }

        // TODO: remove, mock intitalization
        trace!("Distribution of initial data");
        if self.distribute {
            if let Some(random_peer) = self.get_random_peer() {
                debug!("Distributing data to random peers");
                match self.data_to_distribute.pop_back() {
                    Some((id, data)) => {
                        debug!("Sending data with id {:?} to {}", id, random_peer);
                        if let Err(e) = self
                            .consensus
                            .push_tx(Transaction::Stored(id.clone(), id.clone()))
                        {
                            warn!("Error registering transaction about node holding data: {:?}\n Skipping..", e);
                        } else {
                            return Poll::Ready(ToSwarm::NotifyHandler {
                                peer_id: random_peer,
                                handler: NotifyHandler::Any,
                                event: HandlerEvent::SendPrimary(Primary::Simple(
                                    Simple::StoreShard((id, data)),
                                )),
                            });
                        }
                    }
                    None => {
                        info!("Finished distributing initial data");
                        self.distribute = false;
                    }
                }
            } else {
                debug!("No peers found, saving locally");
                match self.data_to_distribute.pop_back() {
                    Some((id, data)) => {
                        debug!("Saving data with id {:?}", id);
                        self.save_shard_locally(id, (), data, self.local_peer_id);
                    }
                    None => {
                        info!("Finished distributing initial data");
                        self.distribute = false;
                    }
                }
            }
        }

        Poll::Pending
    }

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm<Self::ConnectionHandler>) {
        match event {
            libp2p::swarm::FromSwarm::ConnectionEstablished(ConnectionEstablished {
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
            libp2p::swarm::FromSwarm::ConnectionClosed(ConnectionClosed {
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
            libp2p::swarm::FromSwarm::AddressChange(_)
            | libp2p::swarm::FromSwarm::DialFailure(_)
            | libp2p::swarm::FromSwarm::ListenFailure(_)
            | libp2p::swarm::FromSwarm::NewListener(_)
            | libp2p::swarm::FromSwarm::NewListenAddr(_)
            | libp2p::swarm::FromSwarm::ExpiredListenAddr(_)
            | libp2p::swarm::FromSwarm::ListenerError(_)
            | libp2p::swarm::FromSwarm::ListenerClosed(_)
            | libp2p::swarm::FromSwarm::NewExternalAddr(_)
            | libp2p::swarm::FromSwarm::ExpiredExternalAddr(_) => (),
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: libp2p::PeerId,
        connection: libp2p::swarm::ConnectionId,
        event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        self.connection_events.push_front(ConnectionEvent {
            peer_id,
            connection,
            event,
        });
    }
}
