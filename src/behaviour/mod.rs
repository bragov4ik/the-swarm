use std::{collections::VecDeque, pin::Pin, sync::Arc, task::Poll};

use futures::{pin_mut, Future, Stream};
use libp2p::{
    swarm::{NetworkBehaviour, ToSwarm},
    PeerId,
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, Notify},
    time::Sleep,
};
use tracing::{debug, error, warn};

use crate::{
    consensus::{graph::GraphWrapper, Transaction},
    handler::{self, Connection},
    processor::single_threaded::Program,
    types::{Sid, Vid},
};

use self::execution::Execution;

pub type OutEvent = Result<Event, Error>;

pub enum Event {}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Cannot continue behaviour operation. Shutdown (and fresh start?) is the most desirable outcome.")]
    UnableToOperate,
}

enum UserEvent {}

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

struct Channel<T> {
    reciever: mpsc::Receiver<T>,
    sender: mpsc::Sender<T>,
}

struct Behaviour {
    // Processes states
    execution: Execution,

    // Triggers/initiators of processes. todo: check if can replace with simple queues and `loop`
    user_inputs: Channel<UserEvent>,
    programs_to_run: Channel<Program>,
    connection_events: VecDeque<ConnectionEvent>,
    // (author, tx)
    // probably will be replaced by consensus itself, since it has the same interface now
    finalized_transactions: GraphWrapper<Vid, Sid, (), ()>,
    consensus_gossip_timer: Pin<Box<Sleep>>,

    // error triggers?? or smth
    connection_errors: VecDeque<ConnectionError>,

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
    fn perform_random_gossip(&mut self) {
        todo!()
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = Connection;
    type OutEvent = OutEvent;

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm<Self::ConnectionHandler>) {
        todo!()
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
        loop {
            let state_updated_notification = self.state_updated.notified();
            pin_mut!(state_updated_notification);
            // Maybe break on Pending?
            state_updated_notification.poll(cx);

            match self.connection_events.pop_back() {
                // serve piece, recieve piece, recieve gossip,
                Some(s) => {
                    todo!();
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

                None => todo!(),
            }
            break;
        }

        // TODO: check if futures::select! is applicable to avoid starvation (??)
        match self.user_inputs.reciever.poll_recv(cx) {
            // schedule program, collect data, distribute data
            Poll::Ready(Some(event)) => todo!(),
            Poll::Ready(None) => todo!(),
            Poll::Pending => (),
        }

        match self.programs_to_run.reciever.poll_recv(cx) {
            Poll::Ready(Some(program)) => {
                if let Err(e) = self.execution.initiate(program) {
                    warn!("{:?}", e);
                }
            }
            Poll::Ready(None) => todo!(),
            Poll::Pending => (),
        }

        let finalized_transactions = self.finalized_transactions;
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
                        Poll::Ready(Err(e)) => {
                            error!("other half of `programs_to_run` was closed, but it's owned by us. it's a bug.");
                            return Poll::Ready(libp2p::swarm::ToSwarm::GenerateEvent(Err(
                                Error::UnableToOperate,
                            )));
                        }
                        Poll::Pending => {
                            error!("`programs_to_run` queue is full, probably mistake code with tasks priority. continuing will skip a transaction, which is unacceptable.");
                            return Poll::Ready(libp2p::swarm::ToSwarm::GenerateEvent(Err(
                                Error::UnableToOperate,
                            )));
                        }
                    }
                }
            },
            Poll::Ready(None) => {
                error!("other half of `finalized_transactions` was closed, but it's owned by us. it's a bug.");
                return Poll::Ready(libp2p::swarm::ToSwarm::GenerateEvent(Err(
                    Error::UnableToOperate,
                )));
            }
            Poll::Pending => (),
        }

        if let Poll::Ready(_) = self.consensus_gossip_timer.as_mut().poll(cx) {
            self.perform_random_gossip()
        }

        Poll::Pending
    }
}
