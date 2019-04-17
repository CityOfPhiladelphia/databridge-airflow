#!/usr/bin/env bash

TRY_LOOP="20"

: "${FIRST_TIME:=false}"

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

AIRFLOW__CELERY__BROKER_URL="pyamqp://$RABBITMQ_USER:$RABBITMQ_PASSWORD@$RABBITMQ_HOST/$RABBITMQ_VHOST"
# Set the fernet environment variable
eval $(python3 /secrets_manager.py --name=airflow-fernet --key=fernet_key --env=AIRFLOW__CORE__FERNET_KEY)
wait_for_port "RabbitMQ" "$RABBITMQ_HOST" "$RABBITMQ_PORT"
wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"

if [ $FIRST_TIME == true]; then
  # Set the schemas variable
  airflow variables --set schemas $AIRFLOW_HOME/schemas/
fi

case "$1" in
  webserver)
    airflow initdb
    exec airflow webserver
    ;;
  worker)
    # Uncomment this when batch is working
    # pip3 install -r requirements.worker.txt
    # Give the webserver time to run initdb.
    sleep 10
    exec airflow "$@"
    ;;
  scheduler)
    sleep 10
    exec airflow "$@"
    ;;
  flower)
    sleep 10
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac
