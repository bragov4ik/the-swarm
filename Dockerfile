# syntax = docker/dockerfile:1.2

FROM rust:1.70.0 as build

COPY Cargo.toml Cargo.lock /volume/
COPY src /volume/src
WORKDIR /volume
RUN --mount=type=cache,target=/root/.cargo/registry --mount=type=cache,target=/volume/target \
    cargo b --release && \
    cp target/release/the-swarm app


FROM debian:11

WORKDIR /app
COPY --from=build /volume/app ./app_rust
COPY input/simple/data.json input/simple/program.json ./input/

RUN adduser nonroot && \
    chown -R nonroot ./ && \
    chmod 0700 ./ && \
    chmod 0500 ./app_rust && \
    mkdir logs && \
    chown nonroot logs/

USER nonroot

ENV RUST_LOG=info

ENTRYPOINT [ "./app_rust", "--", "-i" ]