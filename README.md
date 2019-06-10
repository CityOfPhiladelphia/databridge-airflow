[![Build Status](https://travis-ci.com/CityOfPhiladelphia/databridge-airflow.svg?branch=master)](https://travis-ci.com/CityOfPhiladelphia/databridge-airflow)

# databridge-airflow
Airflow instance for ETL's involving Databridge

## Goal
- Load data into Carto DB to provide API endpoints for Open Data Philly and application development

## Overview
- Jobs are scheduled and triggered by Airflow
- Airflow stores encrypted database credentials and other metadata in a Postgres database. This database is seeded with secrets (database credentials, slack API keys, carto API keys) from AWS Secrets Manager.
- Airflow kicks off jobs in AWS Batch, consisting of two tasks:
    - Extract data from Databridge or Knack and load it into S3 
    - Load data from S3 to Carto.
- The job in AWS Batch uses the following command line tools to execute the task:
    - [databridge-etl-tools](https://github.com/CityOfPhiladelphia/databridge-etl-tools)
    - [extract-knack](https://github.com/CityOfPhiladelphia/extract-knack)

![airflow-layout](assets/Airflow.png)

## How AWS Batch Jobs Work
- First [read the docs](https://docs.aws.amazon.com/batch/latest/userguide/what-is-batch.html) to understand what AWS Batch is at a high level. AWS Batch allows you to run a batch computing job in a containerized environment on AWS. Batch takes care of the autoscaling to create instances when jobs are launched and terminate them when jobs complete.
- Jobs are launched from job definitions. A job definition consists of some configuration for a job. In our case, the most important thing noted in a job definition is the specification of which Docker image to use. Docker images are stored in [AWS ECR](https://aws.amazon.com/ecr/). There is one docker image and job definition for [databridge-etl-tools](https://github.com/CityOfPhiladelphia/databridge-etl-tools) and another for [extract-knack](https://github.com/CityOfPhiladelphia/extract-knack).
- Jobs require a command to know what to execute. The job will terminate when the execution completes. For this project, commands consist of the commands used to run [databridge-etl-tools](https://github.com/CityOfPhiladelphia/databridge-etl-tools) and [extract-knack](https://github.com/CityOfPhiladelphia/extract-knack), as detailed in their respective Github repositories.

## Requirements
- docker-compose
- Docker
- Access to AWS S3, Batch, and AWS Secrets Manager

## Configuration
- Create an EC2 instance from our `airflow` AMI with access to S3, Batch, and AWS Secrets Manager
- To set all of the database connections up, simply pass the SEED_DB environment variable to docker-compose. Airflow's entrypoint will pick up this environment variable, fetch all database secrets from AWS Secrets Manager, and load them into Airflow's local Postgres Database: 
```bash
# Launch the server and seed the database (initial deployment)
SEED_DB=true docker-compose -f docker-compose.dev.yml up

## Launch the server without seeding the database (redeployment)
docker-compose -f docker-compose.dev.yml up
```

## Deployment
- Automated deployment is done by Travis CI and AWS Code Deploy. Anytime a commit is made to the master branch, a Travis CI job runs which tests airflow and redeploys it to a production EC2 instance. For this reason, make sure any development takes place on a branch and is thoroughly QA'd before merging to master. Notifications as to the status of the deployment are posted to #airflow-prod in Slack.

- If you're launching a production environment for the first time, pass the SEED_DB environment variable to seed the RDS database:
```bash
SEED_DB=true docker-compose -f docker-compose.prod.yml up -d
```

## Testing
```bash
source scripts/run_tests.sh
```