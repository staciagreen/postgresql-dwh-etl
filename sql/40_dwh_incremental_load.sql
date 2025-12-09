BEGIN;

WITH bounds AS (
    SELECT
        LEAST(
            (SELECT MIN(sale_date) FROM src_west.sale),
            (SELECT MIN(sale_date) FROM src_east.sale)
        ) AS dmin,
        GREATEST(
            (SELECT MAX(sale_date) FROM src_west.sale),
            (SELECT MAX(sale_date) FROM src_east.sale)
        ) AS dmax
),
ins AS (
    INSERT INTO dwh.dim_date (full_date, year, month, day)
    SELECT gs::date,
           EXTRACT(YEAR  FROM gs),
           EXTRACT(MONTH FROM gs),
           EXTRACT(DAY   FROM gs)
    FROM bounds,
         generate_series(bounds.dmin, bounds.dmax, interval '1 day') AS gs
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.dim_date d WHERE d.full_date = gs::date
    )
    RETURNING 1
)
SELECT COUNT(*) AS new_dates FROM ins;

WITH all_customers AS (
    SELECT 'west' AS branch_code, customer_id, customer_name FROM src_west.customer
    UNION ALL
    SELECT 'east', customer_id, customer_name FROM src_east.customer
),
ins AS (
    INSERT INTO dwh.dim_customer (branch_key, customer_id, customer_name)
    SELECT br.branch_key, c.customer_id, c.customer_name
    FROM all_customers c
    JOIN dwh.dim_branch br ON br.branch_code = c.branch_code
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.dim_customer dc
        WHERE dc.branch_key = br.branch_key
          AND dc.customer_id = c.customer_id
    )
    RETURNING 1
)
SELECT COUNT(*) AS new_customers FROM ins;

WITH all_products AS (
    SELECT 'west' AS branch_code, product_id, product_name, list_price FROM src_west.product
    UNION ALL
    SELECT 'east', product_id, product_name, list_price FROM src_east.product
),
ins AS (
    INSERT INTO dwh.dim_product (branch_key, product_id, product_name, list_price)
    SELECT br.branch_key, p.product_id, p.product_name, p.list_price
    FROM all_products p
    JOIN dwh.dim_branch br ON br.branch_code = p.branch_code
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.dim_product dp
        WHERE dp.branch_key = br.branch_key
          AND dp.product_id = p.product_id
    )
    RETURNING 1
)
SELECT COUNT(*) AS new_products FROM ins;

WITH all_categories AS (
    SELECT 'west' AS branch_code, category_id, category_name FROM src_west.category
    UNION ALL
    SELECT 'east', category_id, category_name FROM src_east.category
),
ins AS (
    INSERT INTO dwh.dim_category (branch_key, category_id, category_name)
    SELECT br.branch_key, c.category_id, c.category_name
    FROM all_categories c
    JOIN dwh.dim_branch br ON br.branch_code = c.branch_code
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.dim_category dc
        WHERE dc.branch_key = br.branch_key
          AND dc.category_id = c.category_id
    )
    RETURNING 1
)
SELECT COUNT(*) AS new_categories FROM ins;

WITH all_bridges AS (
    SELECT 'west' AS branch_code, product_id, category_id FROM src_west.product_category
    UNION ALL
    SELECT 'east', product_id, category_id FROM src_east.product_category
),
ins AS (
    INSERT INTO dwh.bridge_product_category (product_key, category_key)
    SELECT dp.product_key, dc.category_key
    FROM all_bridges b
    JOIN dwh.dim_branch br ON br.branch_code = b.branch_code
    JOIN dwh.dim_product dp ON dp.product_id = b.product_id AND dp.branch_key = br.branch_key
    JOIN dwh.dim_category dc ON dc.category_id = b.category_id AND dc.branch_key = br.branch_key
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.bridge_product_category x
        WHERE x.product_key = dp.product_key
          AND x.category_key = dc.category_key
    )
    RETURNING 1
)
SELECT COUNT(*) AS new_bridge FROM ins;

WITH src AS (
    SELECT
        'west' AS branch_code,
        si.sale_id,
        si.sale_item_id,
        si.product_id,
        si.quantity,
        si.unit_price,
        si.line_amount,
        s.customer_id,
        s.sale_date
    FROM src_west.sale_item si
    JOIN src_west.sale s USING (sale_id)

    UNION ALL

    SELECT
        'east',
        si.sale_id,
        si.sale_item_id,
        si.product_id,
        si.quantity,
        si.unit_price,
        si.line_amount,
        s.customer_id,
        s.sale_date
    FROM src_east.sale_item si
    JOIN src_east.sale s USING (sale_id)
),
ins AS (
    INSERT INTO dwh.fact_sale_item (
        branch_key, date_key, customer_key, product_key,
        sale_id, sale_item_id, quantity, unit_price, line_amount, list_price
    )
    SELECT
        br.branch_key,
        dd.date_key,
        dc.customer_key,
        dp.product_key,
        src.sale_id,
        src.sale_item_id,
        src.quantity,
        src.unit_price,
        src.line_amount,
        dp.list_price
    FROM src
    JOIN dwh.dim_branch   br ON br.branch_code = src.branch_code
    JOIN dwh.dim_date     dd ON dd.full_date = src.sale_date::date
    JOIN dwh.dim_customer dc ON dc.customer_id = src.customer_id
                             AND dc.branch_key = br.branch_key
    JOIN dwh.dim_product  dp ON dp.product_id = src.product_id
                             AND dp.branch_key = br.branch_key
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.fact_sale_item f
        WHERE f.sale_id = src.sale_id
          AND f.product_key = dp.product_key
    )
    RETURNING 1
)
SELECT COUNT(*) AS new_facts FROM ins;

COMMIT;
