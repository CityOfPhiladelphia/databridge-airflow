#!/usr/bin/env bash

set -e

if [ -d /home/ubuntu/databridge-airflow ]; then
    cd /home/ubuntu/databridge-airflow
    sudo docker-compose -f docker-compose.prod.yml down
    rm -rf /home/ubuntu/databridge-airflow
fi