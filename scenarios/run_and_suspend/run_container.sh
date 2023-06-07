#!/bin/bash

cd ../
docker run --rm --network the-swarm-test-net-1 -it $(docker build -q -f the-swarm/Dockerfile .) -i --parity-shards 2
docker network rm the-swarm-test-net-1 || true
