# The Swarm

Swarm of heterogeneous computers with ability to perform distributed computations.

Right now does not use proper consensus, however the consensus can be easily integrated into the system through the defined API when implemented (see `consensus` module). Also all storage is in RAM.

Project was written in the scope of 2022 summer internship in Innopolis University. Schedule and linked reports can be found [here](https://hackmd.io/H1iKRHrdTiCnZi7QLK0wrw).

## Launch instructions
All nodes should be instide LAN, since local area automatic peer discovery is used (with the help of MDNS component of libp2p library).

### Run main node
`cargo run -- -i`

### Run multiple (or one) secondary nodes
`cargo run`

## Demo instructions
In the main node you can execute commands `help`, `distribute`, `execute`, `read all`.

The intended flow:
- launch nodes ([help](https://github.com/bragov4ik/the-swarm/blob/master/README.md#launch-instructions))
- switch to main node
- run `distribute`
- when finished run `execute`
- when done run `read all` to confirm results

The results should be the same as in `src/demo_input/expected_result.json`

## Debugging
Different log levels can be turned on with `RUST_LOG` environment variable. Details see in [tracing-subscriber documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/index.html#filtering-events-with-environment-variables).
Also it is possible to filter logs of message passing by specifying log levels for `channel_recv` or `channel_send` targets (to see logged `send`'s and `recv`'s respectively).
For example, one can run
```
RUST_LOG="channel_recv=DEBUG,channel_send=DEBUG" cargo run --release
```
to see all debug channel logs.

## Future work
- Complete rust graph consensus implementation (for example [this one](https://github.com/jaybutera/rust-hashgraph), there is no event ordering and the code is not optimized, I tried to solve it in [fork](https://github.com/bragov4ik/rust-hashgraph), but it would take too much time)
- Integrate the consensus (for example) into the system using api in `consensus` module (details in documentation there)
- Update `Behaviour` accordingly (e.g. remove code needed only for mock)
- Add dynamic data/instruction upload
- Improve data ID system (to not add new data each instruction, for example)
- Add proper data erasure coding
- Add disk storage option to work with larger data
