#!/usr/bin/env bash

set -e

# Enter the application's directory
cd /home/ubuntu/databridge-airflow

# Rebuild and restart the server
sudo docker-compose -f docker-compose.prod.yml up -d --build