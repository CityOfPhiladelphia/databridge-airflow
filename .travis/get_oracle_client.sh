#!/usr/bin/env bash

pip install awscli --upgrade --user

# instant basic-lite instant oracle client
aws s3api get-object \
    --bucket citygeo-oracle-instant-client \
    --key oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
        oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm 

# instant oracle-sdk
aws s3api get-object \
    --bucket citygeo-oracle-instant-client \
    --key oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
        oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm