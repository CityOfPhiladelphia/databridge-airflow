# databridge-airflow
Airflow instance for ETL's involving Databridge

## Goal
- Load data from the Oracle SDE version of DataBridge into the new Postgres DataBridge.
- Load data into Carto DB

## Overview
- Jobs are scheduled and triggered by Airflow
- Jobs consist of data extraction, transformation, and loading - primarily through using [geopetl](https://github.com/CityOfPhiladelphia/geopetl) in a AWS Batch job.
- Airflow consists of four main components:
    - webserver - UI to schedule and check on jobs
    - [flower](https://flower.readthedocs.io/en/latest/) - celery monitoring tool
    - scheduler - schedules jobs
    - worker - performs jobs
- Jobs are pushed to a RabbitMQ task queue by Airflow
- Airflow stores encrypted database credentials and other metadata in a Postgres database
- All secrets (database credentials, slack API keys, carto API keys) are stored in AWS Secrets Manager. Airflow's database encryption key (fernet key) is fetched from AWS Secrets Manager when Airflow is launched.

## Setup
- Install docker and git on an EC2 instance and clone this repo
- Make sure your EC2 instance has security groups to access S3, DataBridge's RDS, and AWS Secrets Manager
- Launch Airflow, RabbitMQ, and Postgres all using docker compose `docker-compose up`
- Add the following connections to Airflow. You can find all their information in AWS Secrets Manager:
    - brt_viewer
    - carto_phl
    - databridge
    - databridge2
    - slack