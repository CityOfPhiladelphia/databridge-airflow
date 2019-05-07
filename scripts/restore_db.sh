#!/usr/bin/env bash

set -e

# Enter the application's directory
cd /home/ubuntu/databridge-airflow

# Copy the latest database backup
aws s3 cp s3://citygeo-airflow-databridge2/pgdata pgdata --recursive