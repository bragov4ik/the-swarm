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
use tracing::{debug, trace, warn};

use crate::protocol::{Error as ProtocolError, Message, SwarmComputerProtocol, Request, Response, Simple, Incoming};

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
    RequestReceived(Request),
    ResponseReceived(Request, Response),
    SimpleReceived(Simple),
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
    incoming: Option<BoxFuture<'static, Result<(NegotiatedSubstream, Incoming), ProtocolError>>>,

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
    Protocol(ProtocolError),
    // Tried to start request-response sequence with response
    InitiatedResponse,
    // Peer sent `msg` instead of response
    ResponseExpected(Message),
}

impl std::error::Error for HandlerError {}

impl Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerError::Connection(e) => write!(f, "{}", e),
            HandlerError::Protocol(e) => write!(f, "{}", e),
            HandlerError::InitiatedResponse => write!(f, "Cannot send response before request"),
            HandlerError::ResponseExpected(m) => write!(f, "Peer expected to provide response, however sent {:?}", m),
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

    /// Sends the given message. If it is a request, also waits
    /// for a response from the connection.
    /// 
    /// Response returned together with sent request to provide
    /// context for handling the payload later (e.g. to know which
    /// vector does the received shard belongs to).
    async fn handle_outgoing_message(
        stream: NegotiatedSubstream,
        to_send: Message,
    ) -> Result<(NegotiatedSubstream, Option<(Request, Response)>), HandlerError> {
        let stream = SwarmComputerProtocol::send_message(stream, &to_send).await
            .map_err(HandlerError::Protocol)?;
        match to_send {
            Message::Incoming(Incoming::Simple(_)) => Ok((stream, None)),
            Message::Incoming(Incoming::Request(sent)) => {
                let (s, received) =
                    SwarmComputerProtocol::receive_message::<NegotiatedSubstream, Message>(stream).await
                        .map_err(HandlerError::Protocol)?;
                match received {
                    Message::Outgoing(received) => Ok((s, Some((sent, received)))),
                    Message::Incoming(_) => Err(HandlerError::ResponseExpected(received)),
                }
                
            },
            Message::Outgoing(_) => Err(HandlerError::InitiatedResponse),
        }
    }
}

enum OutgoingState<S> {
    /// Waiting for requests to send
    Idle(S),
    /// Initiated sending request (optionally, get response) in stored future.
    /// Request is also returned as context for processing later.
    Active{out_handler: BoxFuture<'static, Result<(S, Option<(Request, Response)>), HandlerError>>, timer: Pin<Box<Sleep>>},
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
        trace!("Inbound protocol negotiated, setting up channel");
        self.incoming = Some(Self::InboundProtocol::receive_message(protocol).boxed());
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        protocol: <Self::OutboundProtocol as OutboundUpgradeSend>::Output,
        _info: Self::OutboundOpenInfo,
    ) {
        trace!("Outbound protocol negotiated, setting up channel");
        self.outgoing = Some(OutgoingState::Idle(protocol));
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        trace!("Received event {:?}", event);
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
        trace!("Error on upgrading connection: {}", error);
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
        trace!("Checking state");
        match self.state {
            State::PeerUnsupported {
                event_emitted: true,
            } => {
                trace!("Peer doesn't support our connection protocol, nothing more to do");
                return Poll::Pending;
            }
            State::PeerUnsupported {
                event_emitted: false,
            } => {
                warn!("Peer doesn't support our connection protocol, reporting");
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
        trace!("Polling incoming handler's future");
        if let Some(fut) = self.incoming.as_mut() {
            match fut.poll_unpin(cx) {
                Poll::Pending => trace!("Pending, skipping"),
                Poll::Ready(Err(e)) => {
                    debug!("Inbound ping error: {:?}", e);
                    self.incoming = None;
                }
                Poll::Ready(Ok((stream, msg))) => {
                    // Message received, passing it further and start waiting for a new one
                    self.incoming = Some(Self::InboundProtocol::receive_message(stream).boxed());
                    let success = match msg {
                        Incoming::Request(r) => ConnectionSuccess::RequestReceived(r),
                        Incoming::Simple(s) => ConnectionSuccess::SimpleReceived(s),
                    };
                    return Poll::Ready(ConnectionHandlerEvent::Custom(Ok(
                        success
                    )));
                }
            }
        }

        loop {
            // Check for outgoing failures
            if let Some(error) = self.error_queue.pop_back() {
                debug!("Out failure: {:?}", error);
                self.errors_in_row += 1;
                if self.errors_in_row >= self.max_errors {
                    debug!("Too many failures ({}). Closing connection.", self.errors_in_row);
                    return Poll::Ready(ConnectionHandlerEvent::Close(HandlerError::Connection(
                        error,
                    )));
                }
            }

            // Continue outgoing messages.
            trace!("Handling outbound channel");
            match self.outgoing.take() {
                Some(OutgoingState::Active{out_handler: mut fut, mut timer}) => match fut.poll_unpin(cx) {
                    Poll::Pending => {
                        trace!("Pending, checking timeout");
                        match timer.as_mut().poll(cx) {
                            Poll::Ready(_) => {
                                trace!("Timed out");
                                self.error_queue.push_front(ConnectionError::Timeout)
                            },
                            Poll::Pending => {
                                trace!("Still have time");
                                self.outgoing = Some(OutgoingState::Active{out_handler: fut, timer});
                                break;
                            },
                        }
                    }
                    Poll::Ready(Ok((stream, response))) => {
                        trace!("Finished request successfully. Response: {:?}", response);
                        self.errors_in_row = 0;
                        self.outgoing = Some(OutgoingState::Idle(stream));
                        if let Some((req, resp)) = response {
                            return Poll::Ready(ConnectionHandlerEvent::Custom(Ok(
                                ConnectionSuccess::ResponseReceived(req, resp),
                            )));
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        trace!("Finished with error");
                        self.error_queue
                            .push_front(ConnectionError::Other(Box::new(e)));
                    }
                },
                Some(OutgoingState::Idle(stream)) => match self.outgoing_message_queue.pop_back() {
                    Some(m) => {
                        trace!("Adding new message to queue");
                        let timer = Box::pin(sleep(self.timeout));
                        self.outgoing = Some(OutgoingState::Active{
                            out_handler: Self::handle_outgoing_message(stream, m).boxed(),
                            timer,
                        });
                    }
                    None => {
                        trace!("Nothing to send");
                        self.outgoing = Some(OutgoingState::Idle(stream));
                        break;
                    }
                },
                Some(OutgoingState::Negotiating) => {
                    trace!("Can't send requests, protocol not yet negotiated");
                    self.outgoing = Some(OutgoingState::Negotiating);
                    break;
                }
                None => {
                    trace!("No state, starting negotiation");
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
