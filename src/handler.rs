use futures::{future::BoxFuture, AsyncRead, Future, FutureExt};
use libp2p::{
    core::{upgrade::NegotiationError, UpgradeError},
    swarm::{
        handler::{InboundUpgradeSend, OutboundUpgradeSend},
        ConnectionHandler, ConnectionHandlerEvent, ConnectionHandlerUpgrErr, NegotiatedSubstream,
        SubstreamProtocol,
    },
};
use std::{collections::VecDeque, fmt::Display, pin::Pin, task::Poll, time::Duration};
use tokio::time::{sleep, Sleep};
use tracing::{debug, error, trace, warn};

use crate::protocol::{
    Error as ProtocolError, Message, Primary, Request, Response, Simple, SwarmComputerProtocol,
};

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
pub enum ConnectionReceived {
    Request(Request),
    Response(Request, Response),
    Simple(Simple),
}

pub struct Connection {
    /// State of the connection
    state: State,

    // TODO: change to handle multiple parallel requests?? first test
    // maybe it works with them somehow (shouldn't though)
    /// State of incoming message stream. `None` if not yet upgraded or for
    /// similar reasons, `Some(_)` if the connection is established
    incoming: Option<IncomingState<NegotiatedSubstream>>,

    // For now queue of fixed size 1 since we aren't supporing multiple
    // concurrent requests.
    response_queue: Option<Response>,

    // TODO: change to list of sent request (that "await" response) that report 'timed out'
    // after a while. This should allow to handle multiple requests at once.
    /// State of outgoing message stream. Some(_) if the connection is established
    /// None if not yet upgraded or for similar reasons.
    outgoing: Option<OutgoingState<NegotiatedSubstream>>,

    /// Messages to send. Since events are injected independently from sending the
    /// requests, we (probably) need some buffer not to lose them.
    primary_queue: VecDeque<Primary>,

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
    // Peer sent `msg` instead of response
    ResponseExpected(Message),
}

impl std::error::Error for HandlerError {}

impl Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerError::Connection(e) => write!(f, "{}", e),
            HandlerError::Protocol(e) => write!(f, "{}", e),
            HandlerError::ResponseExpected(m) => {
                write!(f, "Peer expected to provide response, however sent {:?}", m)
            }
        }
    }
}

impl Connection {
    pub fn new(max_errors: u64) -> Self {
        Connection {
            incoming: None,
            response_queue: None,
            outgoing: None,
            primary_queue: VecDeque::new(),
            error_queue: VecDeque::new(),
            state: State::Ok,
            errors_in_row: 0,
            max_errors,
            timeout: Duration::from_secs(10),
        }
    }

    /// Sends the given message. If it is a request, also waits
    /// for a response from the connection. The message has to
    /// be primary because we can't just send response out of
    /// nowhere
    ///
    /// Response returned together with sent request to provide
    /// context for handling the payload later (e.g. to know which
    /// vector does the received shard belongs to).
    async fn handle_outgoing_message(
        stream: NegotiatedSubstream,
        to_send: Primary,
    ) -> Result<(NegotiatedSubstream, Option<(Request, Response)>), HandlerError> {
        let stream =
            SwarmComputerProtocol::send_message(stream, Message::Primary(to_send.clone()))
                .await
                .map_err(HandlerError::Protocol)?;
        match to_send {
            Primary::Simple(_) => Ok((stream, None)),
            Primary::Request(sent) => {
                let (s, received) =
                    SwarmComputerProtocol::receive_message::<NegotiatedSubstream, Message>(stream)
                        .await
                        .map_err(HandlerError::Protocol)?;
                match received {
                    Message::Secondary(received) => Ok((s, Some((sent, received)))),
                    Message::Primary(_) => Err(HandlerError::ResponseExpected(received)),
                }
            }
        }
    }
}

type SendRequestHandler<S> =
    BoxFuture<'static, Result<(S, Option<(Request, Response)>), HandlerError>>;

// S means stream
enum OutgoingState<S> {
    /// Waiting for requests to send
    Idle(S),
    /// Initiated sending request (optionally, get response) in stored future.
    /// Request is also returned as context for processing later.
    Active {
        out_handler: SendRequestHandler<S>,
        timer: Pin<Box<Sleep>>,
    },
    /// New substream is being instantiated
    Negotiating,
}

enum IncomingState<S> {
    /// Waiting for next message.
    ///
    /// Represented as future that "borrows" the stream inside (not `&mut`, but the
    /// same logic: moves inside and moves back when done).
    /// On resolving, returns next message and the stream back. See
    /// `inject_fully_negotiated_inbound` and `poll` for details.
    Idle(BoxFuture<'static, Result<(S, Message), ProtocolError>>),
    /// Received a request, waiting for response from the system to
    /// send it back
    PreparingResponse(S),
    /// Started sending the response
    SendingResponse(BoxFuture<'static, Result<S, ProtocolError>>),
}

impl<S> IncomingState<S>
where
    S: AsyncRead + Unpin + Send + 'static,
{
    fn idle_receive(stream: S) -> Self {
        IncomingState::Idle(SwarmComputerProtocol::receive_message(stream).boxed())
    }
}

/// Event coming to our handler (most likely from NetworkBehaviour)
#[derive(Debug)]
pub enum IncomingEvent {
    /// Send either a request (and wait for response)
    /// or just a message
    SendPrimary(Primary),

    /// Answer a request
    SendResponse(Response),
}

impl ConnectionHandler for Connection {
    type InEvent = IncomingEvent;
    type OutEvent = Result<ConnectionReceived, ConnectionError>;
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
        self.incoming = Some(IncomingState::idle_receive(protocol));
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
            IncomingEvent::SendResponse(r) => {
                match &self.response_queue {
                    // Not gamebreaking, report & ignore
                    Some(_) => warn!("No room in queue for response {:?}. Probably an attempt to send a response without a request.", r),
                    None => self.response_queue = Some(r),
                }
            }
            IncomingEvent::SendPrimary(p) => self.primary_queue.push_front(p),
        }
    }

    fn inject_dial_upgrade_error(
        &mut self,
        _info: Self::OutboundOpenInfo,
        error: ConnectionHandlerUpgrErr<<Self::OutboundProtocol as OutboundUpgradeSend>::Error>,
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
        // Make sure to set corresponding `self.incoming` state in each match arm
        if let Some(incoming_state) = self.incoming.take() {
            match incoming_state {
                IncomingState::Idle(mut fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => {
                        trace!("Pending, skipping");
                        self.incoming = Some(IncomingState::Idle(fut));
                    }
                    Poll::Ready(Err(e)) => {
                        error!("Inbound request error: {:?}", e);
                        self.incoming = None;
                    }
                    Poll::Ready(Ok((stream, msg))) => {
                        let res = match msg {
                            Message::Primary(Primary::Request(r)) => {
                                self.incoming = Some(IncomingState::PreparingResponse(stream));
                                Some(ConnectionReceived::Request(r))
                            }
                            Message::Primary(Primary::Simple(s)) => {
                                // Wait for the next Primary
                                self.incoming = Some(IncomingState::idle_receive(stream));
                                Some(ConnectionReceived::Simple(s))
                            }
                            Message::Secondary(s) => {
                                warn!("Unexpected secondary (response) received, dropping");
                                debug!("The unexpected message: {:?}", s);
                                None
                            }
                        };
                        if let Some(success) = res {
                            return Poll::Ready(ConnectionHandlerEvent::Custom(Ok(success)));
                        }
                    }
                },
                IncomingState::PreparingResponse(stream) => {
                    match self.response_queue.take() {
                        Some(resp) => {
                            let msg = Message::Secondary(resp);
                            self.incoming = Some(IncomingState::SendingResponse(Box::pin(
                                SwarmComputerProtocol::send_message(stream, msg),
                            )))
                        }
                        None => {
                            // Still waiting
                            self.incoming = Some(IncomingState::PreparingResponse(stream));
                        }
                    }
                }
                IncomingState::SendingResponse(mut fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => {
                        trace!("Still sending response");
                        self.incoming = Some(IncomingState::SendingResponse(fut));
                    }
                    Poll::Ready(Err(e)) => {
                        debug!("Error sending a message");
                        self.incoming = None;
                        self.error_queue
                            .push_front(ConnectionError::Other(Box::new(e)))
                    }
                    Poll::Ready(Ok(stream)) => {
                        self.incoming = Some(IncomingState::idle_receive(stream));
                    }
                },
            }
        }
        // TODO: check do we need to reinitiate incoming if error happened

        loop {
            // Check for outgoing failures
            while let Some(error) = self.error_queue.pop_back() {
                error!("Out failure: {:?}", error);
                self.errors_in_row += 1;
                if self.errors_in_row >= self.max_errors {
                    debug!(
                        "Too many failures ({}). Closing connection.",
                        self.errors_in_row
                    );
                    return Poll::Ready(ConnectionHandlerEvent::Close(HandlerError::Connection(
                        error,
                    )));
                }
            }

            // Continue outgoing messages.
            trace!("Handling outbound channel");
            match self.outgoing.take() {
                Some(OutgoingState::Active {
                    out_handler: mut fut,
                    mut timer,
                }) => match fut.poll_unpin(cx) {
                    Poll::Pending => {
                        trace!("Pending, checking timeout");
                        match timer.as_mut().poll(cx) {
                            Poll::Ready(_) => {
                                trace!("Timed out");
                                self.error_queue.push_front(ConnectionError::Timeout)
                            }
                            Poll::Pending => {
                                trace!("Still have time");
                                self.outgoing = Some(OutgoingState::Active {
                                    out_handler: fut,
                                    timer,
                                });
                                break;
                            }
                        }
                    }
                    Poll::Ready(Ok((stream, response))) => {
                        trace!("Finished request successfully. Response: {:?}", response);
                        self.errors_in_row = 0;
                        self.outgoing = Some(OutgoingState::Idle(stream));
                        if let Some((req, resp)) = response {
                            return Poll::Ready(ConnectionHandlerEvent::Custom(Ok(
                                ConnectionReceived::Response(req, resp),
                            )));
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        trace!("Finished with error");
                        self.error_queue
                            .push_front(ConnectionError::Other(Box::new(e)));
                    }
                },
                Some(OutgoingState::Idle(stream)) => match self.primary_queue.pop_back() {
                    Some(m) => {
                        trace!("Adding new message to queue");
                        let timer = Box::pin(sleep(self.timeout));
                        self.outgoing = Some(OutgoingState::Active {
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
