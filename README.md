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

## Installation
- Create an EC2 instance with access to S3, Batch, and AWS Secrets Manager
- Update your machine
```bash
sudo apt-get update -yqq
```
- [Install Docker](https://docs.docker.com/install/linux/docker-ce/ubuntu/) and [git](https://www.liquidweb.com/kb/install-git-ubuntu-16-04-lts/)
```bash
 sudo apt-get install -yqq --no-install-recommends \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg-agent \
        software-properties-common \
    && curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add - \
    && sudo add-apt-repository \
        "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
        $(lsb_release -cs) \
        stable" \
    && sudo apt-get install -yqq --no-install-recommends \
        git-core \
        docker-ce \
        docker-ce-cli \
        containerd.io
```
- [Install docker-compose](https://docs.docker.com/compose/install/)
```bash
sudo curl -L "https://github.com/docker/compose/releases/download/1.24.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
    && sudo chmod +x /usr/local/bin/docker-compose
```
- Clone this repo and enter its directory
```bash
git clone https://github.com/CityOfPhiladelphia/databridge-airflow
cd databridge-airflow
```
- Fetch Airflow's database password from AWS Secrets Manager or Lastpass, make up a secure password for RABBITMQ and put them both in a .env file like below:
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