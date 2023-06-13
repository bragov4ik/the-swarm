# The Swarm

Swarm of heterogeneous computers with ability to perform distributed computations.

Utilizes [our implementation](https://github.com/bragov4ik/rust-hashgraph) of [Hashgraph consensus algorithm](https://www.swirlds.com/downloads/SWIRLDS-TR-2016-01.pdf).

Currently supports only addition, subtraction, and inversion in Galois field of order $2^8$. (Basically only addition, because subtraction is exactly addition, and inversion is equality).

This project was written as Bachelor's degree thesis within Innopolis University in 2022-2023 academic year. Link for the paper will be placed [here] later.

Also it was started as 2022 Summer internship at Innopolis University. Schedule and linked reports for the internship can be found [here](https://hackmd.io/H1iKRHrdTiCnZi7QLK0wrw).

## Launch instructions
All nodes should be inside single LAN, because local area automatic peer discovery is used (with the help of [mDNS from libp2p](https://docs.libp2p.io/concepts/discovery-routing/mdns/)).

### Manual run
Launch $N$ nodes, where $N$ is $n+k$ from [Reed-Solomon encoding settings](./src/main.rs) [(particular code line)](https://github.com/bragov4ik/the-swarm/blob/6bed47734008be7f24eb7e69c28b82de9af6618e/src/main.rs#L81)

#### Launch in terminal

```
RUST_LOG=info cargo run --release -- -i
```

Stop each process separately

#### Launch in docker

```
docker run --rm -it $(docker build -q .)
```

Stop each container separately

### Run with docker compose
Create a network (needed because it's used in test scenarios), then 

```
docker network create the-swarm-test-net-1
docker compose up -d
```

When finished, you can stop containers and remove the network:

```
docker compose down
docker network rm the-swarm-test-net-1
```

## Test scenarios launch
The tests are written for [Expect] utility. Thus, it's required to run them.

All tests except for [run_and_suspend](./scenarios/run_and_suspend/) require other peers to be launched manually (instructions for this scenario are described in [Section "Run and suspend test"](#run-and-suspend-test)). In other words, for 3 peer network, the launch process can be the following:
1. Launch 2 peers with `cargo run --release -- -i`
2. Start the script with `./scenarios/run_simple.exp`
3. Wait for the result.

Note that launch arguments/enabled features may vary for different test cases. For example, [test for large data](./scenarios/run_large.exp) requires `big-array` feature to be enabled: `cargo run --release --features big-array -- -i`. Appropriate command for a particular test you can find in the script files.

### "Run and suspend" test

The test is aimed to check if the network works without one peer.

This test case automatically runs containers in a docker network with `compose`. To run use [run script](./scenarios/run_and_suspend/run_and_suspend.sh):
```
./scenarios/run_and_suspend/run_and_suspend.sh
```

If something failed during execution, you can use [cleanup script](./scenarios/run_and_suspend/cleanup.sh) to get rid of residual containers/networks:
```
./scenarios/run_and_suspend/cleanup.sh
```

## CLI options
See `--help` for descriptions.

## Interactive mode commands
Use `help` command to see the list with descriptions.

## Debugging
Different log levels can be turned on with `RUST_LOG` environment variable. Details see in [tracing-subscriber documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/index.html#filtering-events-with-environment-variables).

It is possible to filter logs of message passing by specifying log levels for `channel_recv` or `channel_send` targets (to see logged `send`'s and `recv`'s respectively).

Also different workflows have separate targets: 
| Target               | Workflow               |
|----------------------|------------------------|
| storage_init         | Storage initialization |
| data_distr           | Data distribution      |
| program_exec         | Program execution      |
| data_recollect       | Data recollection      |
| sync                 | Peer synchronization   |

For example, one can run
```
RUST_LOG="channel_recv=DEBUG,channel_send=DEBUG,program_exec=TRACE" cargo run --release
```
to see all debug channel logs, as well as all program execution logs.

## Future work
- Add disk storage option to work with larger data.
- Use other erasure coding method to allow more instructions.
- Make less assumptions in terms of security of the system.
- Consider other errors apart from peer turning off, such as unintentional computation errors.
- Flexible number of peers.
- Flexible encoding.
- Automatic shard loss detection. Addressing the loss before some data is lost.
- Optimize this implementation and consensus.
- Support larger data sizes (currently limited by stack).
- Support not fully (directly) connected networks. Maybe use [relays](https://docs.libp2p.io/concepts/nat/circuit-relay/) or something else.
