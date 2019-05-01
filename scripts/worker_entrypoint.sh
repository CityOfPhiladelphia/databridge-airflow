#!/usr/bin/env bash

# Resolve hostname to fix OID generation error by cx_Oracle
curl -s http://169.254.169.254/latest/meta-data/local-hostname | cut -d. -f1 | awk '{print $1" localhost"}' > /tmp/HOSTALIASES

# This is needed to use both an entrypoint and a command with Docker
exec "${@}"
