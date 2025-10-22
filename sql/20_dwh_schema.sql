CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.dim_branch (
  branch_key  BIGSERIAL PRIMARY KEY,
  branch_code TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
  date_key  BIGSERIAL PRIMARY KEY,
  full_date DATE NOT NULL UNIQUE,
  year      INT  NOT NULL,
  month     INT  NOT NULL,
  day       INT  NOT NULL,
  week      INT  NOT NULL,
  isodow    INT  NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_dim_date_full_date ON dwh.dim_date(full_date);

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
  customer_key   BIGSERIAL PRIMARY KEY,
  branch_key     BIGINT NOT NULL REFERENCES dwh.dim_branch(branch_key),
  customer_id    BIGINT NOT NULL,
  customer_name  TEXT   NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_customer_src ON dwh.dim_customer(branch_key, customer_id);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
  product_key   BIGSERIAL PRIMARY KEY,
  branch_key    BIGINT NOT NULL REFERENCES dwh.dim_branch(branch_key),
  product_id    BIGINT NOT NULL,
  product_name  TEXT   NOT NULL,
  list_price    NUMERIC(12,2) NOT NULL CHECK (list_price >= 0)
);
CREATE INDEX IF NOT EXISTS ix_product_src ON dwh.dim_product(branch_key, product_id);

CREATE TABLE IF NOT EXISTS dwh.dim_category (
  category_key   BIGSERIAL PRIMARY KEY,
  branch_key     BIGINT NOT NULL REFERENCES dwh.dim_branch(branch_key),
  category_id    BIGINT NOT NULL,
  category_name  TEXT   NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_category_src ON dwh.dim_category(branch_key, category_id);


CREATE TABLE IF NOT EXISTS dwh.bridge_product_category (
  product_key  BIGINT NOT NULL REFERENCES dwh.dim_product(product_key) ON DELETE CASCADE,
  category_key BIGINT NOT NULL REFERENCES dwh.dim_category(category_key) ON DELETE CASCADE,
  PRIMARY KEY (product_key, category_key)
);

CREATE TABLE IF NOT EXISTS dwh.fact_sale_item (
  fact_sk       SERIAL PRIMARY KEY,
  branch_sk     BIGINT NOT NULL REFERENCES dwh.dim_branch(branch_sk),
  date_sk       BIGINT NOT NULL REFERENCES dwh.dim_date(date_sk),
  customer_sk   BIGINT NOT NULL REFERENCES dwh.dim_customer(customer_sk),
  product_sk    BIGINT NOT NULL REFERENCES dwh.dim_product(product_sk),

  sale_id       BIGINT NOT NULL,
  sale_item_id  BIGINT NOT NULL,


  quantity      NUMERIC(12,3) NOT NULL CHECK (quantity > 0),
  unit_price    NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),  -- фактическая цена продажи
  list_price    NUMERIC(12,2) NOT NULL CHECK (list_price >= 0),  -- прайсовая на момент продажи
  line_amount   NUMERIC(14,2) NOT NULL CHECK (line_amount >= 0),

  CONSTRAINT uq_fact_sale_item_nat UNIQUE (branch_sk, sale_id, sale_item_id)
);
