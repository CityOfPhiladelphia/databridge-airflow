# databridge-airflow
Airflow instance for ETL's involving Databridge

## Goal
- Load data from the Oracle SDE version of DataBridge into the new Postgres DataBridge (DataBridge2).
- Geo-enrich data in DataBridge2
- Provide data quality checks to ensure empty tables or tables with missing data are not pushed to production
- Load data into Carto DB

## Overview
- Jobs are scheduled and triggered by Airflow
- Jobs consist of data extraction, transformation, and loading - primarily through using [databridge-etl-tools](https://github.com/CityOfPhiladelphia/databridge-etl-tools) in a AWS Batch job.
- Jobs are pushed to a RabbitMQ task queue by Airflow
- Airflow stores encrypted database credentials and other metadata in a Postgres database
- All secrets (database credentials, slack API keys, carto API keys) are stored in AWS Secrets Manager. These are fetched from AWS Secrets Manager when Airflow is launched.

## Setup
- Install docker, docker-compose and git on an EC2 instance and clone this repo
- Make sure your EC2 instance has security groups to access S3, DataBridge's RDS, Batch, and AWS Secrets Manager
- Fetch Airflow's database password from AWS Secrets Manager or Lastpass
- Launch Airflow, RabbitMQ, and Postgres all using docker compose:
```bash
# Make sure to pass in the POSTGRES_PASSWORD
POSTGRES_PASSWORD=postgrespassword docker-compose up
```

To run Airflow in the background use:
```bash
POSTGRES_PASSWORD=postgrespassword docker-compose up -d
```
- To set all of the database connections up, simply passing the SEED_DB environment variable to docker-compose. Airflow's entrypoint will pick up this environment variable, fetch all database secrets from AWS Secrets Manager, and load them into Airflow's local Postgres Database: 
```bash
SEED_DB=true POSTGRES_PASSWORD=postgrespassword docker-compose up
```

- If you're launching a production environment, pass the PROD environment variable to use the appropriate slack channelf for alerts:
```bash
PROD=true SEED_DB=true POSTGRES_PASSWORD=postgrespassword docker-compose up
```