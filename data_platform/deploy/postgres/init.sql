-- Idempotent: runs on every `docker compose up`.

SELECT 'CREATE DATABASE warehouse'        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'warehouse')        \gexec
SELECT 'CREATE DATABASE dagster'          WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dagster')          \gexec
SELECT 'CREATE DATABASE ducklake_catalog' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ducklake_catalog') \gexec

-- Read-only role for MCPs and agents. pg_read_all_data covers current and future tables.
SELECT format('CREATE ROLE agent_ro LOGIN PASSWORD %L', :'agent_pw')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'agent_ro') \gexec
GRANT pg_read_all_data TO agent_ro;

-- Julio's personal read-write login (not superuser). pg_write_all_data covers current and future tables.
SELECT format('CREATE ROLE julio LOGIN PASSWORD %L', :'julio_pw')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'julio') \gexec
GRANT pg_read_all_data, pg_write_all_data TO julio;
GRANT CREATE ON DATABASE warehouse TO julio;

-- Dagster's internal storage is not for agents or people.
REVOKE CONNECT ON DATABASE dagster FROM PUBLIC;
