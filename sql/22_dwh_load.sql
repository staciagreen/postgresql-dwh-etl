BEGIN;
INSERT INTO dwh.dim_branch(branch_code) VALUES ('west'), ('east');

WITH bounds AS (
  SELECT
    LEAST( (SELECT MIN(sale_date) FROM src_west.sale),
           (SELECT MIN(sale_date) FROM src_east.sale) ) AS dmin,
    GREATEST( (SELECT MAX(sale_date) FROM src_west.sale),
              (SELECT MAX(sale_date) FROM src_east.sale) ) AS dmax
)
INSERT INTO dwh.dim_date(full_date, year, month, day)
SELECT d::date,
       EXTRACT(YEAR FROM d)::int,
       EXTRACT(MONTH FROM d)::int,
       EXTRACT(DAY FROM d)::int
FROM bounds b
CROSS JOIN generate_series(b.dmin, b.dmax, interval '1 day') AS g(d)
ORDER BY 1;

INSERT INTO dwh.dim_customer(branch_key, customer_id, customer_name)
SELECT br.branch_key, c.customer_id, c.customer_name
FROM src_west.customer c
JOIN dwh.dim_branch br ON br.branch_code='west'
UNION ALL
SELECT br.branch_key, c.customer_id, c.customer_name
FROM src_east.customer c
JOIN dwh.dim_branch br ON br.branch_code='east';

INSERT INTO dwh.dim_product(branch_key, product_id, product_name, list_price)
SELECT br.branch_key, p.product_id, p.product_name, p.list_price
FROM src_west.product p
JOIN dwh.dim_branch br ON br.branch_code='west'
UNION ALL
SELECT br.branch_key, p.product_id, p.product_name, p.list_price
FROM src_east.product p
JOIN dwh.dim_branch br ON br.branch_code='east';

INSERT INTO dwh.dim_category(branch_key, category_id, category_name)
SELECT br.branch_key, c.category_id, c.category_name
FROM src_west.category c
JOIN dwh.dim_branch br ON br.branch_code='west'
UNION ALL
SELECT br.branch_key, c.category_id, c.category_name
FROM src_east.category c
JOIN dwh.dim_branch br ON br.branch_code='east';

INSERT INTO dwh.bridge_product_category(product_key, category_key)
SELECT dp.product_key, dc.category_key
FROM src_west.product_category pc
JOIN dwh.dim_branch br ON br.branch_code='west'
JOIN dwh.dim_product  dp ON dp.branch_key = br.branch_key AND dp.product_id = pc.product_id
JOIN dwh.dim_category dc ON dc.branch_key = br.branch_key AND dc.category_id = pc.category_id
UNION ALL
SELECT dp.product_key, dc.category_key
FROM src_east.product_category pc
JOIN dwh.dim_branch br ON br.branch_code='east'
JOIN dwh.dim_product  dp ON dp.branch_key = br.branch_key AND dp.product_id = pc.product_id
JOIN dwh.dim_category dc ON dc.branch_key = br.branch_key AND dc.category_id = pc.category_id
ON CONFLICT DO NOTHING;

INSERT INTO dwh.fact_sale_item(
  branch_key, date_key, customer_key, product_key,
  sale_id, sale_item_id, quantity, unit_price, line_amount, list_price
)
SELECT br.branch_key, dd.date_key, dc.customer_key, dp.product_key,
       s.sale_id, si.sale_item_id, si.quantity, si.unit_price, si.line_amount, dp.list_price
FROM src_west.sale_item si
JOIN src_west.sale s   ON s.sale_id = si.sale_id
JOIN dwh.dim_branch br ON br.branch_code = 'west'
JOIN dwh.dim_date   dd ON dd.full_date = s.sale_date
JOIN dwh.dim_customer dc ON dc.branch_key = br.branch_key AND dc.customer_id = s.customer_id
JOIN dwh.dim_product  dp ON dp.branch_key = br.branch_key AND dp.product_id = si.product_id
UNION ALL
SELECT br.branch_key, dd.date_key, dc.customer_key, dp.product_key,
       s.sale_id, si.sale_item_id, si.quantity, si.unit_price, si.line_amount, dp.list_price   -- ← добавили
FROM src_east.sale_item si
JOIN src_east.sale s   ON s.sale_id = si.sale_id
JOIN dwh.dim_branch br ON br.branch_code = 'east'
JOIN dwh.dim_date   dd ON dd.full_date = s.sale_date
JOIN dwh.dim_customer dc ON dc.branch_key = br.branch_key AND dc.customer_id = s.customer_id
JOIN dwh.dim_product  dp ON dp.branch_key = br.branch_key AND dp.product_id = si.product_id;

COMMIT;