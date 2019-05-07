#!/usr/bin/env bash

set -e

# Enter the application's directory
cd /home/ubuntu/databridge-airflow

# Dump to s3
if [ -d "/home/ubuntu/databridge-airflow/pgdata" ]; then
    mkdir -p /home/ubuntu/databridge-airflow/pgdata
    aws s3 cp /home/ubuntu/databridge-airflow/pgdata/ s3://citygeo-airflow-databridge2/pgdata --recursive
fi