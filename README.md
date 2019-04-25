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
- Make sure your EC2 instance has security groups to access S3, DataBridge's RDS, and AWS Secrets Manager
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

## Creating a new standard DAG
- Create a JSON schema file and put it in the *schemas* directory
    - This includes 
        - All fields and their datatypes
        - A primary key
        - A schedule interval for Airflow
        - A key-value pair of *"carto_table_name": false* if the dataset should **not** be uploaded to Carto (optional)
- Copy the JSON schema file to S3 so the AWS Batch worker can pick it up
- Airflow should automatically detect the presence of this schema and generate the new dag. If this doesn't happen, restart the webserver.