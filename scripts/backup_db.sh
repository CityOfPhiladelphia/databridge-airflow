#!/usr/bin/env bash

set -e

# Enter the application's directory
cd /home/ubuntu/databridge-airflow

# Dump to s3
if [ -d "pgdata" ]; then
    aws s3 cp pgdata/ s3://citygeo-airflow-databridge2/pgdata --recursive
fi