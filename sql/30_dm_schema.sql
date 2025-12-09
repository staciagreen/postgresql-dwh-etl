CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS dm.dim_week (
  week_key         BIGSERIAL PRIMARY KEY,
  iso_year         INT    NOT NULL,
  iso_week         INT    NOT NULL,
  week_start_date  DATE   NOT NULL,
  week_end_date    DATE   NOT NULL,
  CONSTRAINT uq_dim_week UNIQUE (iso_year, iso_week),
  CONSTRAINT ck_iso_week CHECK (iso_week BETWEEN 1 AND 53)
);

CREATE INDEX IF NOT EXISTS ix_dim_week_start ON dm.dim_week(week_start_date);
CREATE INDEX IF NOT EXISTS ix_dim_week_uniq  ON dm.dim_week(iso_year, iso_week);

CREATE TABLE IF NOT EXISTS dm.fact_week_sales (
  week_sales_key      BIGSERIAL PRIMARY KEY,
  branch_key           BIGINT NOT NULL REFERENCES dwh.dim_branch(branch_key),
  week_key            BIGINT NOT NULL REFERENCES dm.dim_week(week_key),

  revenue_total       NUMERIC(16,2) NOT NULL,
  items_qty_total     NUMERIC(16,3) NOT NULL,
  orders_count        INT           NOT NULL,
  customers_count     INT           NOT NULL,

  avg_check           NUMERIC(16,2) NOT NULL,
  avg_items_per_order NUMERIC(16,3) NOT NULL,

  CONSTRAINT uq_week_branch UNIQUE (branch_key, week_key)
);
