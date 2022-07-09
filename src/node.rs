use std::{
    collections::{HashMap, HashSet, VecDeque},
    pin::Pin,
    task::Poll,
    time::Duration,
};

use crate::{
    consensus::{DataDiscoverer, GraphConsensus},
    data_memory::DataMemory,
    handler::{Connection, ConnectionError, ConnectionReceived, IncomingEvent as HandlerEvent},
    processor::{Instruction, Processor},
    protocol::{Primary, Request, Response, Simple},
    types::{Shard, Vid},
};
use futures::Future;
use libp2p::{
    swarm::{NetworkBehaviour, NetworkBehaviourAction, NotifyHandler},
    PeerId,
};
use rand::Rng;
use tokio::time::{sleep, Sleep};
use tracing::{debug, error, warn, trace};

struct ConnectionEvent {
    peer_id: libp2p::PeerId,
    connection: libp2p::core::connection::ConnectionId,
    event: Result<ConnectionReceived, ConnectionError>,
}

pub struct Behaviour<TConsensus, TDataMemory, TProcessor>
where
    TDataMemory: DataMemory,
{
    consensus: TConsensus,
    data_memory: TDataMemory,
    _processor: TProcessor,

    /// Random gossip
    connected_peers: HashSet<PeerId>,
    rng: rand::rngs::ThreadRng,
    consensus_gossip_timer: Pin<Box<Sleep>>,
    consensus_gossip_timeout: Duration,

    /// Execution status (to be removed/completely changed with
    /// actual consensus, it's a mock part)
    exec_state: ExecutionState<TDataMemory::Data, TDataMemory::Identifier>,
    pending_handler_events: VecDeque<(PeerId, HandlerEvent)>,

    connection_events: VecDeque<ConnectionEvent>,
    // TODO: timeout, ensure uniqueness/validity?
    incoming_shards_buffer: HashMap<Vid, VecDeque<Shard>>,
}

enum ExecutionState<OP, ID> {
    WaitingData {
        instruction: Instruction<(ID, Option<OP>), ID>,
    },
    WaitingInstruction,
}

impl<C, D, P> Behaviour<C, D, P>
where
    D: DataMemory,
    C: DataDiscoverer<DataIdentifier = Vid, PeerAddr = PeerId>,
{
    pub fn new(
        consensus: C,
        data_memory: D,
        _processor: P,
        consensus_gossip_timeout: Duration,
    ) -> Self {
        Self {
            consensus,
            data_memory,
            _processor,
            connected_peers: HashSet::new(),
            rng: rand::thread_rng(),
            consensus_gossip_timer: Box::pin(sleep(consensus_gossip_timeout)),
            consensus_gossip_timeout,
            exec_state: ExecutionState::WaitingInstruction,
            pending_handler_events: VecDeque::new(),
            connection_events: VecDeque::new(),
            incoming_shards_buffer: HashMap::new(),
        }
    }

    /// Notify behaviour that peer is connected
    pub fn inject_peer_connected(&mut self, new_peer: PeerId) {
        if self.connected_peers.contains(&new_peer) {
            warn!(
                "Tried to set peer {} as connected when it is already in the list",
                new_peer
            );
        } else {
            self.connected_peers.insert(new_peer);
        }
    }

    /// Notify behaviour that peer is disconnected
    pub fn inject_peer_disconnected(&mut self, peer: &PeerId) {
        if !self.connected_peers.remove(peer) {
            warn!(
                "Tried to mark peer {} as disconnected when it's already marked that",
                peer
            );
        }
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

/// No peer found for vector `Vid`
#[derive(Debug)]
struct NoPeerFound(Vid);

impl<TConsensus, TDataMemory, TProcessor> NetworkBehaviour
    for Behaviour<TConsensus, TDataMemory, TProcessor>
where
    // Operator = Vid because we don't store actual data in the consensus
    TConsensus: GraphConsensus<Graph = crate::types::Graph, Operator = Vid>
        + DataDiscoverer<DataIdentifier = <TDataMemory as DataMemory>::Identifier, PeerAddr = PeerId>
        + 'static,
    TDataMemory: DataMemory<Identifier = Vid, Data = Shard> + 'static,
    TProcessor: Processor<Id = Vid, Operand = <TDataMemory as DataMemory>::Data> + 'static,
{
    type ConnectionHandler = Connection;
    type OutEvent = ();

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        Connection::new(10)
    }

    fn inject_event(
        &mut self,
        peer_id: libp2p::PeerId,
        connection: libp2p::core::connection::ConnectionId,
        event: Result<ConnectionReceived, ConnectionError>,
    ) {
        self.connection_events.push_front(ConnectionEvent {
            peer_id,
            connection,
            event,
        });
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        _params: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
        // Maybe later split request handling, gossiping, processing into different behaviours

        trace!("Checking pending handler events to send");
        match self.pending_handler_events.pop_back() {
            Some((addr, e)) => {
                return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
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
                            let result = self.data_memory.get(&id).cloned();
                            return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
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
                                }
                            }
                            None => {
                                debug!("Received shard but vector {:?} is not reconstructed", id)
                            }
                        },
                        ConnectionReceived::Simple(Simple::GossipGraph(graph)) => {
                            match self.consensus.update_graph(graph) {
                                Ok(()) => {}
                                Err(err) => warn!("Error updating graph with gossip: {:?}", err),
                            }
                        }
                    },
                    Err(ConnectionError::PeerUnsupported) => {
                        return Poll::Ready(NetworkBehaviourAction::CloseConnection {
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
                        return Poll::Ready(NetworkBehaviourAction::CloseConnection {
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
                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                        peer_id: random_peer,
                        handler: NotifyHandler::Any,
                        event: HandlerEvent::SendPrimary(Primary::Simple(Simple::GossipGraph(
                            self.consensus.get_graph(),
                        ))),
                    });
                }
            }
            Poll::Pending => {
                // Just wait
            }
        }

        trace!("Distributing received shards");
        match &mut self.exec_state {
            ExecutionState::WaitingInstruction => {}
            ExecutionState::WaitingData { instruction } => {
                let buf = &mut self.incoming_shards_buffer;
                match instruction {
                    Instruction::And((id1, first), (id2, second), _) => {
                        Self::retrieve_from_buf(buf, id1, first);
                        Self::retrieve_from_buf(buf, id2, second);
                    }
                    Instruction::Or((id1, first), (id2, second), _) => {
                        Self::retrieve_from_buf(buf, id1, first);
                        Self::retrieve_from_buf(buf, id2, second);
                    }
                    Instruction::Not((id, shard), _) => Self::retrieve_from_buf(buf, id, shard),
                }
            }
        }

        trace!("Checking computations scheduled");
        match &self.exec_state {
            ExecutionState::WaitingData { instruction } => {
                // `Some(<instruction>)` if all operands are retrieved and we're ready to execute it
                let ready_instruction = match instruction {
                    Instruction::And((_, Some(o1)), (_, Some(o2)), dest) => {
                        Some(Instruction::And(o1, o2, dest))
                    }
                    Instruction::Or((_, Some(o1)), (_, Some(o2)), dest) => {
                        Some(Instruction::Or(o1, o2, dest))
                    }
                    Instruction::Not((_, Some(o)), dest) => Some(Instruction::Not(o, dest)),
                    _ => None,
                };
                if let Some(ready_instruction) = ready_instruction {
                    match <TProcessor as Processor>::execute(&ready_instruction) {
                        Ok(res) => {
                            let dest_id = (*ready_instruction.get_dest()).clone();
                            if self.data_memory.get(&dest_id).is_some() {
                                warn!("Tried to overwrite data in instruction, the execution result is not saved")
                            } else {
                                match self.data_memory.put(dest_id, res) {
                                    Ok(None) => {}
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
                    // Now we need to obtain data for computations. We try to get it from local storage,
                    // if unsuccessful, discover & send requests to corresponding nodes.
                    let state_instruction = match instruction {
                        Instruction::And(i1, i2, dest) | Instruction::Or(i1, i2, dest) => {
                            Instruction::And(
                                (i1.clone(), self.data_memory.get(&i1).cloned()),
                                (i2.clone(), self.data_memory.get(&i2).cloned()),
                                dest,
                            )
                        }
                        Instruction::Not(i, dest) => {
                            Instruction::Not((i.clone(), self.data_memory.get(&i).cloned()), dest)
                        }
                    };

                    // Schedule data requests, if needed
                    let success = match &state_instruction {
                        Instruction::And((i1, opt1), (i2, opt2), _)
                        | Instruction::Or((i1, opt1), (i2, opt2), _) => {
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
                        Instruction::Not((i, opt), _) => {
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

        // TODO: add cli interaction
        Poll::Pending
    }
}