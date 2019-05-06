#!/usr/bin/env bash

set -e

# Enter the application's directory
cd /home/ubuntu/databridge-airflow

# Dump to s3
aws s3 cp pgdata/ s3://citygeo-airflow-databridge2/pgdata --recursive