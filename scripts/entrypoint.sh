#!/usr/bin/env bash

TRY_LOOP="20"

# Run set_secrets.py and set outputs as environment variables
eval $(python3 set_secrets.py | sed 's/^/export /')

: "${RABBITMQ_HOST:="rabbitmq"}"
: "${RABBITMQ_PORT:="5672"}"
: "${RABBITMQ_USER:="airflow"}"
: "${RABBITMQ_PASSWORD:="airflow"}"
: "${RABBITMQ_VHOST:="airflow"}"

: "${POSTGRES_HOST:="postgres"}"
: "${POSTGRES_PORT:="5432"}"
: "${POSTGRES_USER:="airflow"}"
: "${POSTGRES_PASSWORD:="airflow"}"
: "${POSTGRES_DB:="airflow"}"

: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Celery}Executor}"

export \
  AIRFLOW__CELERY__BROKER_URL \
  AIRFLOW__CELERY__RESULT_BACKEND \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__SQL_ALCHEMY_CONN \
  AIRFLOW_HOME \

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

AIRFLOW__CELERY__BROKER_URL="amqp://$RABBITMQ_USER:$RABBITMQ_PASSWORD@$RABBITMQ_HOST:$RABBITMQ_PORT/$RABBITMQ_VHOST"
AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
wait_for_port "RabbitMQ" "$RABBITMQ_HOST" "$RABBITMQ_PORT"
wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"
