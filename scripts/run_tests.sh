#!/usr/bin/env bash

docker build -f Dockerfile.test -t test .

docker run test