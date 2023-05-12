use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub trait Module {
    type InEvent;
    type OutEvent;
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

/// Created with [`ModuleChannelServer::new()`]
pub struct ModuleChannelClient<M: Module> {
    pub input: mpsc::Sender<M::InEvent>,
    pub output: mpsc::Receiver<M::OutEvent>,
    state: Option<Arc<Mutex<M::SharedState>>>,
}

impl<M: Module> ModuleChannelClient<M> {
    pub fn accepts_input(&self) -> bool {
        let Some(state) = &self.state else {
            return true
        };
        let Ok(state) = state.try_lock() else {
            return false;
        };
        state.accepts_input()
    }
}
