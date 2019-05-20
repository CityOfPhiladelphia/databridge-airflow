#!/usr/bin/env bash

docker build -t airflow .

docker run -it airflow --entrypoint "pytest -p no:warnings"