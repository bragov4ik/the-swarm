# The Swarm

Swarm of heterogeneous computers with ability to perform distributed computations.

Right now does not use proper consensus, however the consensus can be easily integrated into the system through the defined API when implemented (see `consensus` module).

Project was written in the scope of 2022 summer internship in Innopolis University. Schedule and linke reports can be found [here](https://hackmd.io/H1iKRHrdTiCnZi7QLK0wrw).

## Launch instructions
All nodes should be instide LAN, since local area automatic peer discovery is used.

### Run main node
`cargo run -- -i`

### Run multiple (or one) secondary nodes
`cargo run`

## Demo instructions
In the main node you can execute commands `help`, `distribute`, `execute`, `read all`.

The intended flow:
- launch nodes
- run `distribute`
- when finished run `execute`
- when done run `read all` to confirm results

## Debugging
Different log levels can be turned on with `RUST_LOG` environment variable. Details see in [tracing-subscriber documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/index.html#filtering-events-with-environment-variables).

## Future work
- Complete rust graph consensus implementation (for example [this one](https://github.com/jaybutera/rust-hashgraph), there is no event ordering and the code is not optimized)
- Integrate the consensus into the system using api in `consensus` module (details in documentation there)
- ..?
