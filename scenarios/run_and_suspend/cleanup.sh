#!/bin/bash

docker compose down
docker network rm the-swarm-test-net-1 || true
