BEGIN;

TRUNCATE dm.fact_week_sales, dm.dim_week RESTART IDENTITY;

WITH base AS (
  SELECT
    d.full_date,
    EXTRACT(ISOYEAR FROM d.full_date)::int AS iso_year,
    EXTRACT(WEEK    FROM d.full_date)::int AS iso_week,
    EXTRACT(ISODOW  FROM d.full_date)::int AS isodow
  FROM dwh.dim_date d
),
weeks AS (
  SELECT
    iso_year,
    iso_week,
    (MIN(full_date) - (MIN(isodow)-1) * INTERVAL '1 day')::date AS week_start_date,
    ((MIN(full_date) - (MIN(isodow)-1) * INTERVAL '1 day') + INTERVAL '6 day')::date AS week_end_date
  FROM base
  GROUP BY iso_year, iso_week
)
INSERT INTO dm.dim_week(iso_year, iso_week, week_start_date, week_end_date)
SELECT iso_year, iso_week, week_start_date, week_end_date
FROM weeks
ORDER BY iso_year, iso_week;

INSERT INTO dm.fact_week_sales(
  branch_sk, week_key,
  revenue_total, items_qty_total, orders_count, customers_count,
  avg_check, avg_items_per_order
)
SELECT
  f.branch_sk,
  w.week_key,
  SUM(f.line_amount)::numeric(16,2)          AS revenue_total,
  SUM(f.quantity)::numeric(16,3)             AS items_qty_total,
  COUNT(DISTINCT f.sale_id)                  AS orders_count,
  COUNT(DISTINCT f.customer_sk)              AS customers_count,
  CASE WHEN COUNT(DISTINCT f.sale_id)=0
       THEN 0::numeric(16,2)
       ELSE (SUM(f.line_amount) / COUNT(DISTINCT f.sale_id))::numeric(16,2)
  END                                        AS avg_check,
  CASE WHEN COUNT(DISTINCT f.sale_id)=0
       THEN 0::numeric(16,3)
       ELSE (SUM(f.quantity) / COUNT(DISTINCT f.sale_id))::numeric(16,3)
  END                                        AS avg_items_per_order
FROM dwh.fact_sale_item f
JOIN dwh.dim_date d
  ON d.date_sk = f.date_sk
JOIN dm.dim_week w
  ON w.iso_year = EXTRACT(ISOYEAR FROM d.full_date)::int
 AND w.iso_week = EXTRACT(WEEK    FROM d.full_date)::int
GROUP BY f.branch_sk, w.week_key
ORDER BY w.week_key, f.branch_sk;

COMMIT;
