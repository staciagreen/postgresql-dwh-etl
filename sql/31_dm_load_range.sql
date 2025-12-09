CREATE OR REPLACE PROCEDURE dm.load_week_sales_range(
    p_start_date DATE,
    p_end_date   DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM dm.fact_week_sales f
    USING dm.dim_week w
    WHERE f.week_key = w.week_key
      AND w.week_start_date >= p_start_date
      AND w.week_end_date   <= p_end_date;

    DELETE FROM dm.dim_week
    WHERE week_start_date >= p_start_date
      AND week_end_date   <= p_end_date;

    WITH base AS (
        SELECT gs::date AS full_date,
               EXTRACT(ISOYEAR FROM gs)::int AS iso_year,
               EXTRACT(WEEK    FROM gs)::int AS iso_week,
               EXTRACT(ISODOW  FROM gs)::int AS isodow
        FROM generate_series(p_start_date, p_end_date, interval '1 day') AS gs
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
    ORDER BY iso_year, iso_week
    ON CONFLICT (iso_year, iso_week) DO NOTHING;

    INSERT INTO dm.fact_week_sales(
        branch_key, week_key,
        revenue_total, items_qty_total, orders_count, customers_count,
        avg_check, avg_items_per_order
    )
    SELECT
        f.branch_key,
        w.week_key,
        SUM(f.line_amount)::numeric(16,2),
        SUM(f.quantity)::numeric(16,3),
        COUNT(DISTINCT f.sale_id),
        COUNT(DISTINCT f.customer_key),
        CASE WHEN COUNT(DISTINCT f.sale_id)=0
             THEN 0
             ELSE (SUM(f.line_amount) / COUNT(DISTINCT f.sale_id))::numeric(16,2)
        END,
        CASE WHEN COUNT(DISTINCT f.sale_id)=0
             THEN 0
             ELSE (SUM(f.quantity) / COUNT(DISTINCT f.sale_id))::numeric(16,3)
        END
    FROM dwh.fact_sale_item f
    JOIN dwh.dim_date d ON d.date_key = f.date_key
    JOIN dm.dim_week w
      ON w.iso_year = EXTRACT(ISOYEAR FROM d.full_date)::int
     AND w.iso_week = EXTRACT(WEEK    FROM d.full_date)::int
    WHERE d.full_date BETWEEN p_start_date AND p_end_date
    GROUP BY f.branch_key, w.week_key
    ORDER BY w.week_key, f.branch_key;

END;
$$;
