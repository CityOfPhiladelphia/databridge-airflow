# databridge-airflow
Airflow instance for ETL's involving Databridge

## Goal
- Load data from the Oracle SDE version of DataBridge into the new Postgres DataBridge (DataBridge2).
- Geo-enrich data in DataBridge2
- Provide data quality checks to ensure empty tables or tables with missing data are not pushed to production
- Load data into Carto DB

## Overview
- Jobs are scheduled and triggered by Airflow
- Jobs consist of data extraction, transformation, and loading - primarily through using [geopetl](https://github.com/CityOfPhiladelphia/geopetl) in a AWS Batch job.
- Jobs are pushed to a RabbitMQ task queue by Airflow
- Airflow stores encrypted database credentials and other metadata in a Postgres database
- All secrets (database credentials, slack API keys, carto API keys) are stored in AWS Secrets Manager. These are fetched from AWS Secrets Manager when Airflow is launched.

## Setup
- Install docker, docker-compose and git on an EC2 instance and clone this repo
- Make sure your EC2 instance has security groups to access S3, DataBridge's RDS, Batch, and AWS Secrets Manager
- Launch Airflow, RabbitMQ, and Postgres all using docker compose:
```bash
docker-compose up
```

To run Airflow in the background use:
```bash
docker-compose up -d
```
- To set all of the database connections up, simply passing the SEED_DB environment variable to docker-compose. Airflow's entrypoint will pick up this environment variable, fetch all database secrets from AWS Secrets Manager, and load them into Airflow's local Postgres Database: 
```bash
SEED_DB=true docker-compose up
```

- If you're launching a production environment, pass the PROD environment variable to use the appropriate slack channelf for alerts:
```bash
PROD=true docker-compose up
```

## Run an ETL locally
- Build the Docker image:
```bash
docker build -t airflow-worker -f Dockerfile.worker .
```
- Launch a bash shell inside the Docker container (requires overridding the entrypoint)
```bash
docker run -it airflow-worker --entrypoint /bin/bash airflow-worker
```
- Run your ETL command
```bash
python3 /extract_and_load_to_databridge.py \ 
       write \
       db_type=postgres \
       db_host=$DB_HOST \
       db_user=$DB_USER \
       db_password=$DB_PASSWORD \
       db_name=$DB_NAME \
       db_port=5432 \
       db_table_schema=lni \
       db_table_name=li_imm_dang \
       s3_bucket=$S3_BUCKET
```