#!/usr/bin/env bash

TRY_LOOP="20"

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

AIRFLOW__CELERY__BROKER_URL="pyamqp://$RABBITMQ_USER:$RABBITMQ_PASSWORD@$RABBITMQ_HOST/$RABBITMQ_VHOST"

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

delete_default_airflow_connections() {
  declare -a DEFAULT_CONNS=(
        "cassandra_default"
        "azure_cosmos_default"
        "azure_data_lake_default"
        "segment_default"
        "qubole_default"
        "databricks_default"
        "emr_default"
        "sqoop_default"
        "redis_default"
        "druid_ingest_default"
        "druid_broker_default"
        "spark_default"
        "aws_default"
        "fs_default"
        "sftp_default"
        "ssh_default"
        "webhdfs_default"
        "wasb_default"
        "vertica_default"
        "mssql_default"
        "http_default"
        "sqlite_default"
        "postgres_default"
        "mysql_default"
        "mongo_default"
        "metastore_default"
        "hiveserver2_default"
        "hive_cli_default"
        "google_cloud_default"
        "presto_default"
        "bigquery_default"
        "beeline_default"
	"local_mysql"
      )

  for CONN in "${DEFAULT_CONNS[@]}"
  do
    airflow connections --delete --conn_id $CONN
  done
}       

set_environment_variables() {
  # Set the fernet environment variable
  eval $(python3 /secrets_manager.py --name=airflow-fernet --key=fernet_key --env=AIRFLOW__CORE__FERNET_KEY)
  # Set environment variables for connections
  
  # brt-viewer
  eval $(python3 /secrets_manager.py --name=brt-viewer --key=host --env=BRT_VIEWER_HOST)
  eval $(python3 /secrets_manager.py --name=brt-viewer --key=username --env=BRT_VIEWER_LOGIN)
  eval $(python3 /secrets_manager.py --name=brt-viewer --key=password --env=BRT_VIEWER_PASSWORD)
  eval $(python3 /secrets_manager.py --name=brt-viewer --key=dbname --env=BRT_VIEWER_EXTRA)

  # carto_phl
  eval $(python3 /secrets_manager.py --name=carto-prod --key=username --env=CARTO_PHL_LOGIN)
  eval $(python3 /secrets_manager.py --name=carto-prod --key=connection_string --env=CARTO_PHL_PASSWORD)

  # databridge
  eval $(python3 /secrets_manager.py --name=databridge --key=host --env=DATABRIDGE_HOST)
  eval $(python3 /secrets_manager.py --name=databridge --key=username --env=DATABRIDGE_LOGIN)
  eval $(python3 /secrets_manager.py --name=databridge --key=password --env=DATABRIDGE_PASSWORD)
  eval $(python3 /secrets_manager.py --name=databridge --key=dbname --env=DATABRIDGE_EXTRA)

  # databridge2
  eval $(python3 /secrets_manager.py --name=databridge-raw --key=host --env=DATABRIDGE2_HOST)
  eval $(python3 /secrets_manager.py --name=databridge-raw --key=username --env=DATABRIDGE2_LOGIN)
  eval $(python3 /secrets_manager.py --name=databridge-raw --key=password --env=DATABRIDGE2_PASSWORD)
  eval $(python3 /secrets_manager.py --name=databridge-raw --key=dbname --env=DATABRIDGE2_EXTRA)

  # hansen
  #eval $(python3 /secrets_manager.py --name=hansen --key=host --env=HANSEN_HOST)
  #eval $(python3 /secrets_manager.py --name=hansen --key=username --env=HANSEN_LOGIN)
  #eval $(python3 /secrets_manager.py --name=hansen --key=password --env=HANSEN_PASSWORD)
  #eval $(python3 /secrets_manager.py --name=hansen --key=dbname --env=HANSEN_EXTRA)

  # slack
  eval $(python3 /secrets_manager.py --name=airflow-slack-dev --key=airflow-slack-dev --env=SLACK_EXTRA)
}

set_airflow_connections() {
  airflow connections \
	  --add --conn_id brt-viewer \
	  --conn_type oracle \
	  --conn_host $BRT_VIEWER_HOST \
	  --conn_login $BRT_VIEWER_LOGIN \
	  --conn_password $BRT_VIEWER_PASSWORD \
	  --conn_port 1521 \
	  --conn_extra $BRT_VIEWER_EXTRA
  airflow connections \
	  --add --conn_id carto_phl \
	  --conn_type carto \
	  --conn_login $CARTO_PHL_LOGIN \
	  --conn_password $CARTO_PHL_PASSWORD
  airflow connections \
	  --add --conn_id databridge \
          --conn_type oracle \
          --conn_host $DATABRIDGE_HOST \
          --conn_login $DATABRIDGE_LOGIN \
          --conn_password $DATABRIDGE_PASSWORD \
          --conn_port 1521 \
	  --conn_extra $DATABRIDGE_EXTRA
  airflow connections \
          --add --conn_id "databridge2" \
          --conn_type postgres \
          --conn_host $DATABRIDGE2_HOST \
          --conn_login $DATABRIDGE2_LOGIN \
          --conn_password $DATABRIDGE2_PASSWORD \
          --conn_port 5432 \
	  --conn_extra $DATABRIDGE2_EXTRA
  #airflow connections \
         # --add --conn_id hansen \
        #  --conn_type oracle \
        #  --conn_host HANSEN_HOST \
        #  --conn_login HANSEN_LOGIN \
        #  --conn_password HANSEN_PASSWORD \
        #  --conn_port 1521
  airflow connections \
	  --add --conn_id slack \
	  --conn_type slack \
	  --conn_extra $SLACK_EXTRA
}    

wait_for_port "RabbitMQ" "$RABBITMQ_HOST" "$RABBITMQ_PORT"
wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"

case "$1" in
  webserver)
    airflow initdb
    if [ -z "$FIRST_TIME" ]; then
      delete_default_airflow_connections
      set_environment_variables
      # Set the schemas variable
      airflow variables --set schemas $AIRFLOW_HOME/schemas/
      set_airflow_connections
    fi
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
