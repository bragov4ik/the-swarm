use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

pub trait Module {
    type InEvent;
    type OutEvent;
    /// Works only if `state` is `Some(_)` in `ModuleChannelServer`.
    /// (i.e. if `ModuleChannelServer::new()` was given
    /// `Some(_)` as `initial_state`)
    type SharedState: State;
}

pub trait State {
    fn accepts_input(&self) -> bool;
}

impl State for () {
    fn accepts_input(&self) -> bool {
        true
    }
}

pub struct ModuleChannelServer<M: Module> {
    pub input: mpsc::Receiver<M::InEvent>,
    pub output: mpsc::Sender<M::OutEvent>,
    pub state: Option<Arc<Mutex<M::SharedState>>>,
    pub shutdown: CancellationToken,
}

impl<M> ModuleChannelServer<M>
where
    M: Module,
{
    /// If `state` is `None`, it is not checked and `accepts_input()`
    /// will always be true
    pub fn new(
        initial_state: Option<M::SharedState>,
        buffer: usize,
        shutdown: CancellationToken,
    ) -> (ModuleChannelServer<M>, ModuleChannelClient<M>) {
        let (input_send, input_recv) = mpsc::channel(buffer);
        let (output_send, output_recv) = mpsc::channel(buffer);
        let state_shared = initial_state.map(|init| Arc::new(Mutex::new(init)));
        let server = ModuleChannelServer {
            input: input_recv,
            output: output_send,
            state: state_shared.clone(),
            shutdown,
        };
        let client = ModuleChannelClient {
            input: input_send,
            output: output_recv,
            state: state_shared,
        };
        (server, client)
    }

    /// Set the state to given value.
    /// If the server was created with `None` state - does nothing
    pub fn set_state(&mut self, value: M::SharedState) {
        let Some(state) = &mut self.state else {
            return;
        };
        let timeout = Duration::from_millis(100);
        let Some(mut state) = state.try_lock_for(timeout) else {
            warn!("State mutex was locked for >{:?}, shouldn't happen", timeout);
            return;
        };
        *state = value;
    }
}

/// Created with [`ModuleChannelServer::new()`]
pub struct ModuleChannelClient<M: Module> {
    pub input: mpsc::Sender<M::InEvent>,
    pub output: mpsc::Receiver<M::OutEvent>,
    state: Option<Arc<Mutex<M::SharedState>>>,
}

impl<M: Module> ModuleChannelClient<M> {
    /// If it was created with `None` state - will always be true
    pub fn accepts_input(&self) -> bool {
        let Some(state) = &self.state else {
            return true
        };
        let Some(state) = state.try_lock() else {
            return false;
        };
        state.accepts_input()
    }
}
