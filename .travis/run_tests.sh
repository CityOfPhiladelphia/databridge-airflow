#!/usr/bin/env bash

docker build -f Dockerfile.travis -t travis .

docker run travis