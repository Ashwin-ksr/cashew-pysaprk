#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 -v user="$METASTORE_USER" -v pw="$METASTORE_PASSWORD" \
     --username postgres <<'SQL'
CREATE USER :"user" WITH PASSWORD :'pw';
CREATE DATABASE metastore OWNER :"user";
SQL
