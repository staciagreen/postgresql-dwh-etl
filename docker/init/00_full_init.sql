CREATE DATABASE branch_west;
CREATE DATABASE branch_east;

\connect branch_west
\i /docker/sql/10_schema.sql
\i /docker/sql/11_fk.sql

\connect branch_east
\i /docker/sql/10_schema.sql
\i /docker/sql/11_fk.sql

CREATE DATABASE dwh;

\connect dwh
\i /docker/sql/20_dwh_schema.sql
\i /docker/sql/21_fdw.sql
