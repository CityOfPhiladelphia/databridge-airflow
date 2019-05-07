#!/usr/bin/env bash

set -e

# Enter the application's directory
cd /home/ubuntu/databridge-airflow

# Dump to s3
if [ -d "/home/ubuntu/databridge-airflow/pgdata" ]; then
    aws s3 cp /home/ubuntu/databridge-airflow/pgdata/ s3://citygeo-airflow-databridge2/pgdata --recursive
    sudo rm -r /home/ubuntu/databridge-airflow/pgdata/
fi