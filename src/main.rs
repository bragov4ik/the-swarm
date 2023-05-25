use clap::Parser;
use futures::StreamExt;
use libp2p::mdns;
use libp2p::swarm::SwarmEvent;
use libp2p::Multiaddr;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};

use std::error::Error;

use crate::network::CombinedBehaviourEvent;

// "modules" as in `module::Module`
mod behaviour;
mod consensus;
mod data_memory;
mod instruction_storage;
mod request_response;

mod encoding;
mod io;
mod logging_helpers;
mod module;
mod network;
mod processor;
mod protocol;
mod signatures;
mod types;
mod ui;

const CHANNEL_BUFFER_LIMIT: usize = 100;
//todo move to spec file
const ENCODING_SETTINGS: encoding::reed_solomon::Settings = encoding::reed_solomon::Settings {
    data_shards_total: 3,
    data_shards_sufficient: 2,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Is this node a main one (that does all the stuff for demo)
    #[clap(short, long)]
    is_main: bool,

    #[clap(long)]
    generate_input: bool,

    #[clap(short, long)]
    dial_address: Option<String>,

    /// Seed to generate key
    #[clap(long)]
    key_seed: Option<u8>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "gen-input")]
    {
        crate::io::test_write_input("data.json", "program.json")
            .await
            .unwrap();
        return Ok(());
    }

    let args = Args::parse();

    let (mut swarm, mut request_response_server, join_handles, shutdown_token) =
        network::new(None).await.unwrap();

    // let format = tracing_subscriber::fmt::format();
    #[cfg(not(feature = "console"))]
    let _guard = {
        let (non_blocking, _guard) = tracing_appender::non_blocking(
            std::fs::File::create(format!("./{}.log", swarm.local_peer_id())).unwrap(),
        );

        let file_layer = tracing_subscriber::fmt::Layer::new().with_writer(non_blocking);

        tracing_subscriber::registry()
            .with(file_layer)
            .with(tracing_subscriber::fmt::layer())
            .init();
        _guard
    };
    #[cfg(feature = "console")]
    console_subscriber::init();

    // Dial the peer identified by the multi-address given as the second
    // command-line argument, if any.
    if let Some(addr) = args.dial_address {
        let remote: Multiaddr = addr.parse()?;
        swarm.dial(remote)?;
        info!("Dialed {}", addr)
    }

    // todo: send sigterm
    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(CombinedBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer, address) in list {
                            swarm.behaviour_mut().main.inject_peer_discovered(peer);
                            swarm.behaviour_mut().request_response.add_address(&peer, address);
                        }
                    }
                    SwarmEvent::Behaviour(CombinedBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                        for (peer, address) in list {
                            if !swarm.behaviour_mut().mdns.has_node(&peer) {
                                swarm.behaviour_mut().main.inject_peer_expired(&peer);
                                swarm.behaviour_mut().request_response.remove_address(&peer, &address);
                            }
                        }
                    }
                    SwarmEvent::Behaviour(CombinedBehaviourEvent::RequestResponse(e)) => {
                        let handle_result = request_response::handle_request_response_event(
                            &mut request_response_server, e
                        ).await;
                        if let Err(_) = handle_result {
                            error!("Shutting down...");
                            shutdown_token.cancel();
                            break;
                        }
                    },
                    SwarmEvent::Behaviour(CombinedBehaviourEvent::Main(Err(behaviour::Error::CancelSignal))) => {
                        info!("{}", behaviour::Error::CancelSignal);
                        shutdown_token.cancel();
                        break;
                    },
                    SwarmEvent::Behaviour(CombinedBehaviourEvent::Main(Err(behaviour::Error::UnableToOperate))) => {
                        error!("Shutting down...");
                        shutdown_token.cancel();
                        break;
                    },
                    SwarmEvent::Behaviour(event) => info!("{:?}", event),
                    SwarmEvent::NewListenAddr { address, .. } => {
                        let local_peer_id = *swarm.local_peer_id();
                        eprintln!(
                            "Local node is listening on {:?}",
                            address.with(libp2p::multiaddr::Protocol::P2p(local_peer_id.into()))
                        );
                    }
                    SwarmEvent::IncomingConnection { .. } => {}
                    SwarmEvent::ConnectionEstablished { .. } => {}
                    SwarmEvent::ConnectionClosed { .. } => {}
                    SwarmEvent::OutgoingConnectionError { .. } => {}
                    SwarmEvent::IncomingConnectionError { .. } => {}
                    SwarmEvent::Dialing(peer_id) => eprintln!("Dialing {peer_id}"),
                    other => debug!("{:?}", other),
                }
            }
            action = request_response_server.input.recv() => {
                let Some(action) = action else {
                    error!("other half of `request_response_server.input` was closed. no reason to operate without main behaviour.");
                    shutdown_token.cancel();
                    break;
                };
                info!("{:?}", action);
                match action {
                    request_response::InEvent::MakeRequest { request, to } => {
                        let request_id = swarm.behaviour_mut().request_response.send_request(&to, request.clone());
                        let send_result = request_response_server.output.send(request_response::OutEvent::AssignedRequestId { request_id, request }).await;
                        if let Err(_) = send_result {
                            error!("other half of `request_response_server.output` was closed. no reason to operate without main behaviour.");
                            shutdown_token.cancel();
                            break;
                        }
                    },
                    request_response::InEvent::Respond { request_id, channel, response } => {
                        let send_result = swarm.behaviour_mut().request_response.send_response(channel, response);
                        if let Err(_) = &send_result {
                            warn!("Could not send response to {:?}: {:?}", request_id, send_result);
                        }
                    },
                }
            }
        }
    }
    for handle in join_handles {
        handle.await.unwrap()
    }
    Ok(())
}
