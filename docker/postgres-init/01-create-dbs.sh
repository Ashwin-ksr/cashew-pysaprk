#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1  \
     -v msuser="$METASTORE_USER" -v mspw="$METASTORE_PASSWORD" \
     -v afuser="$AIRFLOW_DB_USER" -v afpw="$AIRFLOW_DB_PASSWORD" \
     --username postgres <<'SQL'
CREATE USER :"msuser" WITH PASSWORD :'mspw';
CREATE DATABASE metastore OWNER :"msuser";
CREATE USER :"afuser" WITH PASSWORD :'afpw';
CREATE DATABASE airflow OWNER :"afuser";
SQL
