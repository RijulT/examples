#!/bin/bash

docker stop $(docker ps -a -q)
docker run -d -p 9004:9004 edge_analyzer
