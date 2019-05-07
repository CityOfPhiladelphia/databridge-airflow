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

## AWS AMI Installation
- Create an EC2 instance from our `airflow` AMI with access to S3, Batch, and AWS Secrets Manager

## Manual Installation on Ubuntu (no AMI)
** Just use the AMI **
```bash
./scripts/setup.sh
```

## Configuration
- Fetch Airflow's database password from AWS Secrets Manager or Lastpass, make up a secure password for RABBITMQ and put them both in a .env file in this project's directory like below:
```bash
POSTGRES_PASSWORD=postgrespassword
RABBITMQ_DEFAULT_PASS=rabbitmqpassword
```

- To set all of the database connections up, simply passing the SEED_DB environment variable to docker-compose. Airflow's entrypoint will pick up this environment variable, fetch all database secrets from AWS Secrets Manager, and load them into Airflow's local Postgres Database: 
```bash
SEED_DB=true docker-compose up
```
## Development
- Launch Airflow, RabbitMQ, and Postgres all using docker compose:
```bash
docker-compose up
```

To run Airflow in the background use *detached mode*:
```bash
docker-compose up -d
```

## Production
- If you're launching a production environment, pass the PROD environment variable to use the appropriate slack channel for alerts:
```bash
PROD=true SEED_DB=true docker-compose up -d
```

## Deployment
- Automated deployment is done by Travis CI and AWS Code Deploy. Anytime a commit is made to the master branch, airflow is redeployed to a production EC2 instance. For this reason, make sure any development takes place on a branch and is thoroughly QA'd before merging to master.