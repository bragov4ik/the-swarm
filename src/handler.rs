use std::{collections::VecDeque, fmt::Display, task::Poll, time::Duration, pin::Pin};

use futures::{future::BoxFuture, FutureExt, Future};
use tokio::time::{sleep, Sleep};
use libp2p::{
    core::{upgrade::NegotiationError, UpgradeError},
    swarm::{
        ConnectionHandler, ConnectionHandlerEvent, ConnectionHandlerUpgrErr, NegotiatedSubstream,
        SubstreamProtocol, handler::{OutboundUpgradeSend, InboundUpgradeSend},
    },
};

use crate::protocol::{Error as ProtocolError, Message, SwarmComputerProtocol};

// Handler state
enum State {
    Ok,
    // We want to report this fact only once, thus we use the flag
    PeerUnsupported { event_emitted: bool },
}

#[derive(Debug)]
pub enum ConnectionError {
    Timeout,
    PeerUnsupported,
    Other(Box<dyn std::error::Error + Send + 'static>),
}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionError::Timeout => write!(f, "Timed out"),
            ConnectionError::PeerUnsupported => write!(f, "Protocol is not supported by peer"),
            ConnectionError::Other(e) => write!(f, "{}", e),
        }
    }
}

#[derive(Debug)]
pub enum ConnectionSuccess {
    MessageReceived(Message),
}

pub struct Connection {
    /// State of the connection
    state: State,

    /// State of incoming message stream. Some(_) if the connection is established
    /// None if not yet upgraded or for similar reasons.
    ///
    /// Represented as future that "borrows" the stream inside (not `&mut`, but the
    /// same logic: moves inside and moves back when done).
    /// On resolving, returns next message and the stream back. See
    /// `inject_fully_negotiated_inbound` and `poll` to see details.
    incoming: Option<BoxFuture<'static, Result<(NegotiatedSubstream, Message), ProtocolError>>>,

    // TODO: change to list of sent request (that "await" response) that report 'timed out'
    // after a while. This should allow to handle multiple requests at once.
    /// State of outgoing message stream. Some(_) if the connection is established
    /// None if not yet upgraded or for similar reasons.
    outgoing: Option<OutgoingState<NegotiatedSubstream>>,

    /// Messages to send. Since events are injected independently from sending the
    /// requests, we (probably) need some buffer not to lose them.
    outgoing_message_queue: VecDeque<Message>,

    /// Errors to report/count in next `poll()`
    error_queue: VecDeque<ConnectionError>,

    errors_in_row: u64,

    // TODO: move to config
    max_errors: u64,
    timeout: Duration,
}

#[derive(Debug)]
pub enum HandlerError {
    Connection(ConnectionError),
}

impl std::error::Error for HandlerError {}

impl Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerError::Connection(c) => write!(f, "{}", c),
        }
    }
}

impl Connection {
    pub fn new(max_errors: u64) -> Self {
        Connection {
            incoming: None,
            outgoing: None,
            outgoing_message_queue: VecDeque::new(),
            error_queue: VecDeque::new(),
            state: State::Ok,
            errors_in_row: 0,
            max_errors,
            timeout: Duration::from_secs(10),
        }
    }

    /// Sends the given message and depending on its type also may
    /// wait for the response on the same stream.
    async fn handle_outgoing_message(
        stream: NegotiatedSubstream,
        msg: Message,
    ) -> Result<(NegotiatedSubstream, Option<Message>), ProtocolError> {
        let stream = SwarmComputerProtocol::send_message(stream, &msg).await?;
        Ok(match msg {
            Message::Single(_) => (stream, None),
            Message::Pair(_) => {
                let (s, m) =
                    SwarmComputerProtocol::receive_message::<NegotiatedSubstream, Message>(stream)
                        .await?;
                (s, Some(m))
            }
        })
    }
}

enum OutgoingState<S> {
    // Waiting for requests to send
    Idle(S),
    // Initiated sending request (optionally, get response) in stored future
    Active{out_handler: BoxFuture<'static, Result<(S, Option<Message>), ProtocolError>>, timer: Pin<Box<Sleep>>},
    /// New substream is being instantiated
    Negotiating,
}

/// Event coming to our handler (most likely from NetworkBehaviour)
#[derive(Debug)]
pub enum IncomingEvent {
    SendMessage(Message),
}

impl ConnectionHandler for Connection {
    type InEvent = IncomingEvent;
    type OutEvent = Result<ConnectionSuccess, ConnectionError>;
    type Error = HandlerError;
    type InboundProtocol = SwarmComputerProtocol;
    type OutboundProtocol = SwarmComputerProtocol;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(
        &self,
    ) -> libp2p::swarm::SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(SwarmComputerProtocol, ())
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        protocol: <Self::InboundProtocol as InboundUpgradeSend>::Output,
        _info: Self::InboundOpenInfo,
    ) {
        self.incoming = Some(Self::InboundProtocol::receive_message(protocol).boxed());
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        protocol: <Self::OutboundProtocol as OutboundUpgradeSend>::Output,
        _info: Self::OutboundOpenInfo,
    ) {
        self.outgoing = Some(OutgoingState::Idle(protocol));
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        match event {
            IncomingEvent::SendMessage(m) => {
                self.outgoing_message_queue.push_front(m);
            }
        }
    }

    fn inject_dial_upgrade_error(
        &mut self,
        _info: Self::OutboundOpenInfo,
        error: ConnectionHandlerUpgrErr<
            <Self::OutboundProtocol as OutboundUpgradeSend>::Error,
        >,
    ) {
        self.outgoing = None;

        let error = match error {
            ConnectionHandlerUpgrErr::Upgrade(UpgradeError::Select(NegotiationError::Failed)) => {
                self.state = State::PeerUnsupported {
                    event_emitted: false,
                };
                return;
            }
            ConnectionHandlerUpgrErr::Timeout => ConnectionError::Timeout,
            e => ConnectionError::Other(Box::new(e)),
        };
        self.error_queue.push_front(error);
    }

    fn connection_keep_alive(&self) -> libp2p::swarm::KeepAlive {
        // TODO add some logic for disconnecting
        libp2p::swarm::KeepAlive::Yes
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<
        libp2p::swarm::ConnectionHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
            Self::Error,
        >,
    > {
        match self.state {
            State::PeerUnsupported {
                event_emitted: true,
            } => {
                return Poll::Pending;
            }
            State::PeerUnsupported {
                event_emitted: false,
            } => {
                self.state = State::PeerUnsupported {
                    event_emitted: true,
                };
                return Poll::Ready(ConnectionHandlerEvent::Custom(Err(
                    ConnectionError::PeerUnsupported,
                )));
            }
            State::Ok => {}
        }

        // Handle incoming requests
        if let Some(fut) = self.incoming.as_mut() {
            match fut.poll_unpin(cx) {
                Poll::Pending => {}
                Poll::Ready(Err(e)) => {
                    //log::debug!("Inbound ping error: {:?}", e);
                    self.incoming = None;
                }
                Poll::Ready(Ok((stream, msg))) => {
                    // A ping from a remote peer has been answered, wait for the next.
                    self.incoming = Some(Self::InboundProtocol::receive_message(stream).boxed());
                    return Poll::Ready(ConnectionHandlerEvent::Custom(Ok(
                        ConnectionSuccess::MessageReceived(msg),
                    )));
                }
            }
        }

        loop {
            // Check for outgoing failures
            if let Some(error) = self.error_queue.pop_back() {
                // log::debug!("Out failure: {:?}", error);
                self.errors_in_row += 1;
                if self.errors_in_row >= self.max_errors {
                    // log::debug!("Too many failures ({}). Closing connection.", self.errors_in_row);
                    return Poll::Ready(ConnectionHandlerEvent::Close(HandlerError::Connection(
                        error,
                    )));
                }
            }

            // Continue outgoing messages.
            match self.outgoing.take() {
                Some(OutgoingState::Active{out_handler: mut fut, mut timer}) => match fut.poll_unpin(cx) {
                    Poll::Pending => {
                        match timer.as_mut().poll(cx) {
                            Poll::Ready(_) => self.error_queue.push_front(ConnectionError::Timeout),
                            Poll::Pending => {
                                self.outgoing = Some(OutgoingState::Active{out_handler: fut, timer});
                                break;
                            },
                        }
                    }
                    Poll::Ready(Ok((stream, response))) => {
                        self.errors_in_row = 0;
                        self.outgoing = Some(OutgoingState::Idle(stream));
                        if let Some(response) = response {
                            return Poll::Ready(ConnectionHandlerEvent::Custom(Ok(
                                ConnectionSuccess::MessageReceived(response),
                            )));
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        self.error_queue
                            .push_front(ConnectionError::Other(Box::new(e)));
                    }
                },
                Some(OutgoingState::Idle(stream)) => match self.outgoing_message_queue.pop_back() {
                    Some(m) => {
                        let timer = Box::pin(sleep(self.timeout));
                        self.outgoing = Some(OutgoingState::Active{
                            out_handler: Self::handle_outgoing_message(stream, m).boxed(),
                            timer,
                        });
                    }
                    None => {
                        self.outgoing = Some(OutgoingState::Idle(stream));
                        break;
                    }
                },
                Some(OutgoingState::Negotiating) => {
                    self.outgoing = Some(OutgoingState::Negotiating);
                    break;
                }
                None => {
                    self.outgoing = Some(OutgoingState::Negotiating);
                    let protocol = self.listen_protocol().with_timeout(self.timeout);
                    return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                        protocol,
                    });
                }
            }
        }
        Poll::Pending
    }
}
