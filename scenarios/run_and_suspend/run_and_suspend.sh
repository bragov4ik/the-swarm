#!/bin/bash

docker network rm the-swarm-test-net-1 || true
docker network create the-swarm-test-net-1
sleep 1

cd ../
docker compose -f ./the-swarm/docker-compose.yml --project-directory . up --build -d --force-recreate
sleep 1
cd ./the-swarm
path_to_script="./scenarios/run_and_suspend/_run_and_suspend.exp"
"$path_to_script"
cd ../
docker compose -f ./the-swarm/docker-compose.yml --project-directory . down
docker network rm the-swarm-test-net-1 || true
