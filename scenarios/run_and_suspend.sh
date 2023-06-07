#!/bin/bash

# NOT WORKING :(

# Launch the processes in the background
stty -tostop
RUST_LOG="info" cargo run --release -- --parity-shards 2 &
sleep 1
RUST_LOG="info" cargo run --release -- --parity-shards 2 &
sleep 1
RUST_LOG="info" cargo run --release -- --parity-shards 2 &

# Get the PID of the first process
pid=$!

# Run another script with the PID as an argument
path_to_script="./scenarios/_run_and_suspend.exp"
"$path_to_script" "$pid"
