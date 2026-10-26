#!/bin/bash
# Creates the application roles. Passwords come from the container environment.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v api_pw="$API_RO_PASSWORD" -v rw_pw="$CONNECT_RW_PASSWORD" <<'SQL'
CREATE ROLE api_ro LOGIN PASSWORD :'api_pw';
CREATE ROLE connect_rw LOGIN PASSWORD :'rw_pw';

GRANT USAGE ON SCHEMA gold TO api_ro, connect_rw;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO api_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO api_ro;
GRANT SELECT, INSERT, UPDATE ON gold.observations TO connect_rw;
SQL
