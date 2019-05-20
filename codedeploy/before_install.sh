#!/usr/bin/env bash

set -e

if [ -d /home/ubuntu/databridge-airflow ]; then
    rm -rf /home/ubuntu/databridge-airflow
fi