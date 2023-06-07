#!/bin/bash

docker network rm the-swarm-test-net-1 || true
docker network create the-swarm-test-net-1
sleep 1

docker compose up --build -d --force-recreate
sleep 1
path_to_script="./scenarios/run_and_suspend/_run_and_suspend.exp"
"$path_to_script"
echo "Cleaning up..."
&> /dev/null
docker compose down
docker network rm the-swarm-test-net-1 || true
