#!/bin/bash

docker run --rm --network the-swarm-test-net-1 -it $(docker build -q .) -i --parity-shards 2
docker network rm the-swarm-test-net-1 || true
