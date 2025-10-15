CREATE DATABASE dwh;

\connect dwh

CREATE EXTENSION IF NOT EXISTS pgcrypto;


CREATE TABLE IF NOT EXISTS dim_date (
  date_sk      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  full_date    DATE NOT NULL UNIQUE,
  year         INT NOT NULL,
  month        INT NOT NULL,
  day          INT NOT NULL,
  week         INT NOT NULL,
  dow          INT NOT NULL,
  rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
  modifieddate TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_customer (
  customer_sk  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_bk  BIGINT NOT NULL UNIQUE,
  customer_name TEXT NOT NULL,
  rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
  modifieddate TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_product (
  product_sk   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  product_bk   BIGINT NOT NULL UNIQUE,
  product_name TEXT NOT NULL,
  list_price   NUMERIC(12,2) NOT NULL CHECK (list_price >= 0),
  rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
  modifieddate TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_category (
  category_sk  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  category_bk  BIGINT NOT NULL UNIQUE,
  category_name TEXT NOT NULL,
  rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
  modifieddate TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bridge_product_category (
  product_sk   BIGINT NOT NULL,
  category_sk  BIGINT NOT NULL,
  rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
  modifieddate TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_bridge_pc PRIMARY KEY (product_sk, category_sk)
);

CREATE TABLE IF NOT EXISTS fact_sale_item (
  fact_sk        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sale_item_bk   BIGINT NOT NULL UNIQUE,
  sale_bk        BIGINT NOT NULL,
  customer_sk    BIGINT NOT NULL,
  product_sk     BIGINT NOT NULL,
  date_sk        BIGINT NOT NULL,
  quantity       NUMERIC(12,3) NOT NULL CHECK (quantity > 0),
  unit_price     NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
  line_amount    NUMERIC(14,2) NOT NULL CHECK (line_amount >= 0),
  rowguid        UUID NOT NULL DEFAULT gen_random_uuid(),
  modifieddate   TIMESTAMPTZ NOT NULL DEFAULT now()
);
