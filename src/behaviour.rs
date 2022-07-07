use std::{collections::{VecDeque, HashMap}, task::Poll};

use crate::{
    consensus::{DataDiscoverer, GraphConsensus},
    data_memory::DataMemory,
    handler::{Connection, ConnectionError, ConnectionSuccess, IncomingEvent},
    processor::Processor,
    protocol::{Message, Request, Response, Simple},
    types::{Shard, Vid},
};
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviourAction, NotifyHandler};
use tracing::{error, debug, warn};

struct ConnectionEvent {
    peer_id: libp2p::PeerId,
    connection: libp2p::core::connection::ConnectionId,
    event: Result<ConnectionSuccess, ConnectionError>,
}

pub struct Node<TConsensus, TDataMemory, TProcessor> {
    consensus: TConsensus,
    data_memory: TDataMemory,
    processor: TProcessor,

    connection_events: VecDeque<ConnectionEvent>,
    // TODO: timeout, ensure uniqueness/validiry?
    incoming_shards_buffer: HashMap<Vid, VecDeque<Shard>>
}

impl<C, D, P> Node<C, D, P> {
    pub fn new(consensus: C, data_memory: D, processor: P) -> Self {
        Self {
            consensus,
            data_memory,
            processor,
            connection_events: VecDeque::new(),
            incoming_shards_buffer: HashMap::new(),
        }
    }

    async fn retrieve_shards(id: Vid) {}
}

impl<TConsensus, TDataMemory, TProcessor> NetworkBehaviour
    for Node<TConsensus, TDataMemory, TProcessor>
where
    TConsensus: GraphConsensus<Graph = crate::types::Graph> + DataDiscoverer + 'static,
    TDataMemory: DataMemory<Identifier = Vid, Data = Shard> + 'static,
    TProcessor: Processor + 'static,
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
        event: Result<ConnectionSuccess, ConnectionError>,
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
        params: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
        match self.connection_events.pop_back() {
            Some(ConnectionEvent {
                peer_id,
                connection,
                event,
            }) => {
                match event {
                    Ok(success) => {
                        match success {
                            ConnectionSuccess::RequestReceived(Request::Shard(id)) => {
                                debug!("Received request for getting vector {:?} shard, responding", id);
                                let result = self.data_memory.get(&id);
                                return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                                    peer_id,
                                    handler: NotifyHandler::One(connection),
                                    event: IncomingEvent::SendResponse(Response::Shard(result)),
                                });
                            },
                            ConnectionSuccess::ResponseReceived(Request::Shard(id), Response::Shard(shard)) => {
                                match shard {
                                    Some(shard) => {
                                        debug!("Received shard for vector {:?}", id);
                                        if let Some(queue) = self.incoming_shards_buffer.get_mut(&id) {
                                            queue.push_front(shard)
                                        }
                                    },
                                    None => debug!("Received shard but vector {:?} is not reconstructed", id),
                                }
                            },
                            ConnectionSuccess::SimpleReceived(Simple::GossipGraph(graph)) => {
                                match self.consensus.update_graph(graph) {
                                    Ok(()) => {},
                                    Err(err) => warn!("Error updating graph with gossip: {:?}", err),
                                }
                            },
                        }
                    }
                    Err(ConnectionError::PeerUnsupported) => {
                        return Poll::Ready(NetworkBehaviourAction::CloseConnection {
                            peer_id,
                            connection: libp2p::swarm::CloseConnection::One(connection),
                        })
                    }
                    // save stats mb
                    // logged in handler already; also counted there to close conneciton
                    // on too many errors
                    Err(ConnectionError::Timeout) => { },
                    Err(ConnectionError::Other(err)) => {
                        // Fail fast
                        error!("Connection to {} returned error {:?}", peer_id, err);
                        return Poll::Ready(NetworkBehaviourAction::CloseConnection {
                            peer_id,
                            connection: libp2p::swarm::CloseConnection::One(connection),
                        })
                    }
                }
            }
            None => {}
        }
        

        Poll::Pending
    }
}
