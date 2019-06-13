#!/usr/bin/env bash

airflow initdb

airflow connections \
    --add --conn_id brt-viewer \
        --conn_type oracle \
        --conn_host localhost \
        --conn_login login \
        --conn_password password \
        --conn_port 1521 \
        --conn_extra '{"db_name": "db_name"}'
airflow connections \
    --add --conn_id carto_phl \
        --conn_type HTTP \
        --conn_login login \
        --conn_password password
airflow connections \
    --add --conn_id carto_gsg \
        --conn_type HTTP \
        --conn_login login \
        --conn_password password
airflow connections \
    --add --conn_id databridge \
        --conn_type oracle \
        --conn_host localhost \
        --conn_login login \
        --conn_password password \
        --conn_port 1521 \
        --conn_extra '{"db_name": "db_name"}'
airflow connections \
    --add --conn_id "databridge2" \
        --conn_type postgres \
        --conn_host localhost \
        --conn_login login \
        --conn_password password \
        --conn_port 5432 \
        --conn_extra $'{"db_name": "db_name"}' 
#airflow connections \
        # --add --conn_id hansen \
    #  --conn_type oracle \
    #  --conn_host HANSEN_HOST \
    #  --conn_login HANSEN_LOGIN \
    #  --conn_password HANSEN_PASSWORD \
    #  --conn_port 1521
airflow connections \
    --add --conn_id slack \
        --conn_type HTTP \
        --conn_host https://hooks.slack.com/services \
        --conn_password password
airflow connections \
    --add --conn_id knack \
        --conn_type HTTP \
        --conn_login knack_application_id \
        --conn_password knack_api_key
airflow connections \
    --add --conn_id knack_test \
        --conn_type HTTP \
        --conn_login knack_application_id \
        --conn_password knack_api_key
airflow connections \
    --add --conn_id airtable \
        --conn_type HTTP \
        --conn_login login \
        --conn_password password
airflow connections \
    --add --conn_id ais \
        --conn_type HTTP \
        --conn_login login \
        --conn_password password \
        --conn_host hostname

pytest -p no:warnings tests/