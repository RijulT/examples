#!/bin/bash

docker stop $(docker ps -a -q)
docker image rm --force $(docker images edge_analyzer -q)
docker tag $(docker image load --input $1 | cut -c14-) edge_analyzer:latest


