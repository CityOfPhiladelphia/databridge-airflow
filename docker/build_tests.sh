#!/usr/bin/env bash

docker build -t airflow .

chmod +x scripts/test.sh

docker build -f Dockerfile.test -t test .

docker run -it test