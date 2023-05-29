# syntax = docker/dockerfile:1.2

FROM rust:1.69.0 as build

COPY rust-hashgraph/Cargo.toml rust-hashgraph/Cargo.lock /rust-hashgraph/
COPY rust-hashgraph/src /rust-hashgraph/src
COPY the-swarm/Cargo.toml the-swarm/Cargo.lock /volume/
COPY the-swarm/src /volume/src
WORKDIR /volume
RUN --mount=type=cache,target=/root/.cargo/registry --mount=type=cache,target=/volume/target \
    cargo b --release && \
    cp target/release/the-swarm app


FROM alpine:3.16.2

WORKDIR /app
COPY --from=build /volume/app ./app_rust
COPY the-swarm/data.json the-swarm/program.json ./input/

RUN adduser nonroot -D -H && \
    chown -R nonroot ./ && \
    chmod 0700 ./ && \
    chmod 0500 ./app_rust && \
    mkdir logs && \
    chown nonroot logs/

# This user is already included in the image
USER nonroot

ENTRYPOINT [ "./app_rust" ]