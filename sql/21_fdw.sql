\connect dwh;

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SERVER IF EXISTS srv_west CASCADE;
CREATE SERVER srv_west FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (dbname 'branch_west', host '/var/run/postgresql');

DROP SERVER IF EXISTS srv_east CASCADE;
CREATE SERVER srv_east FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (dbname 'branch_east', host '/var/run/postgresql');

CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER srv_west OPTIONS (user 'postgres');
CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER srv_east OPTIONS (user 'postgres');

CREATE SCHEMA IF NOT EXISTS src_west;
CREATE SCHEMA IF NOT EXISTS src_east;

IMPORT FOREIGN SCHEMA public LIMIT TO (customer, product, category, product_category, sale, sale_item)
  FROM SERVER srv_west INTO src_west;

IMPORT FOREIGN SCHEMA public LIMIT TO (customer, product, category, product_category, sale, sale_item)
  FROM SERVER srv_east INTO src_east;