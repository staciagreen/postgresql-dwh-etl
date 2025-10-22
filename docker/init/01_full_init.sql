CREATE DATABASE dwh;

\connect dwh
\i /docker/sql/20_dwh_schema.sql
\i /docker/sql/21_fdw.sql
