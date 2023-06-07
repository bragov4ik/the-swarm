#!/bin/bash

cd ../
docker compose -f ./the-swarm/docker-compose.yml --project-directory . down
docker network rm the-swarm-test-net-1 || true
