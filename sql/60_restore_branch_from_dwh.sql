CREATE TABLE IF NOT EXISTS dwh.restore_log (
  id SERIAL PRIMARY KEY,
  branch_code TEXT NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  customers_inserted INT,
  products_inserted INT,
  categories_inserted INT,
  sales_inserted INT,
  sale_items_inserted INT,
  run_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE PROCEDURE restore_branch_from_dwh(
    p_branch_code TEXT,
    p_start_date  DATE,
    p_end_date    DATE,
    p_force       BOOLEAN DEFAULT false
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_branch_key BIGINT;
    v_count INT;
    v_target_schema TEXT;

    rec RECORD;
    v_new_id BIGINT;
    v_new_cust BIGINT;
    v_new_prod BIGINT;
    v_new_sale BIGINT;

    v_customers INT := 0;
    v_products INT := 0;
    v_categories INT := 0;
    v_sales INT := 0;
    v_sale_items INT := 0;
BEGIN
    IF p_start_date IS NULL OR p_end_date IS NULL OR p_start_date > p_end_date THEN
        RAISE EXCEPTION 'Invalid date range: % - %', p_start_date, p_end_date;
    END IF;

    SELECT branch_key INTO v_branch_key
    FROM dwh.dim_branch
    WHERE branch_code = p_branch_code;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Branch % not found in dwh.dim_branch', p_branch_code;
    END IF;
    v_target_schema := 'src_' || p_branch_code;

    SELECT COUNT(*) INTO v_count
    FROM dwh.fact_sale_item f
    JOIN dwh.dim_date dd ON f.date_key = dd.date_key
    WHERE f.branch_key = v_branch_key
      AND dd.full_date BETWEEN p_start_date AND p_end_date;

    IF v_count = 0 THEN
        RAISE EXCEPTION 'No fact records found in dwh for branch % in date range % - %', p_branch_code, p_start_date, p_end_date;
    END IF;

    IF NOT p_force THEN
        BEGIN
            EXECUTE format('SELECT 1 FROM %I.customer LIMIT 1', v_target_schema) INTO v_count;
            IF v_count IS NOT NULL THEN
                RAISE EXCEPTION 'Target branch "customer" table is not empty - aborting to avoid conflicts. Use p_force=true to override.';
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- if remote query fails, continue (schema may not yet exist)
            NULL;
        END;
    ELSE
        RAISE NOTICE 'p_force=true: skipping emptiness checks for target tables';
    END IF;

    CREATE TEMP TABLE IF NOT EXISTS tmp_customer_map(orig_customer_key BIGINT, orig_customer_id BIGINT, new_customer_id BIGINT) ON COMMIT DROP;
    CREATE TEMP TABLE IF NOT EXISTS tmp_product_map(orig_product_key BIGINT, orig_product_id BIGINT, new_product_id BIGINT) ON COMMIT DROP;
    CREATE TEMP TABLE IF NOT EXISTS tmp_category_map(orig_category_key BIGINT, orig_category_id BIGINT, new_category_id BIGINT) ON COMMIT DROP;
    CREATE TEMP TABLE IF NOT EXISTS tmp_sale_map(orig_sale_id BIGINT, new_sale_id BIGINT) ON COMMIT DROP;

    FOR rec IN
      SELECT DISTINCT dc.customer_key, dc.customer_id, dc.customer_name
      FROM dwh.dim_customer dc
      JOIN dwh.fact_sale_item f ON f.customer_key = dc.customer_key
      JOIN dwh.dim_date dd ON f.date_key = dd.date_key
      WHERE dc.branch_key = v_branch_key
        AND dd.full_date BETWEEN p_start_date AND p_end_date
    LOOP
        v_new_id := NULL;
        BEGIN
            EXECUTE format(
              'INSERT INTO %I.customer (customer_id, customer_name, rowguid, modifieddate) VALUES ($1,$2,$3,$4) RETURNING customer_id',
              v_target_schema
            )
            USING rec.customer_id, rec.customer_name, gen_random_uuid(), NOW()
            INTO v_new_id;
            v_customers := v_customers + 1;
        EXCEPTION WHEN OTHERS THEN
            BEGIN
                EXECUTE format(
                  'INSERT INTO %I.customer (customer_name, rowguid, modifieddate) VALUES ($1,$2,$3) RETURNING customer_id',
                  v_target_schema
                )
                USING rec.customer_name, gen_random_uuid(), NOW()
                INTO v_new_id;
                v_customers := v_customers + 1;
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Customer insert into % failed: %', v_target_schema || '.customer', SQLERRM;
                RAISE;
            END;
        END;

        INSERT INTO tmp_customer_map(orig_customer_key, orig_customer_id, new_customer_id) VALUES (rec.customer_key, rec.customer_id,
                                                                                                   v_new_id);
    END LOOP;

    FOR rec IN
      SELECT DISTINCT dp.product_key, dp.product_id, dp.product_name, dp.list_price
      FROM dwh.dim_product dp
      JOIN dwh.fact_sale_item f ON f.product_key = dp.product_key
      JOIN dwh.dim_date dd ON f.date_key = dd.date_key
      WHERE dp.branch_key = v_branch_key
        AND dd.full_date BETWEEN p_start_date AND p_end_date
    LOOP
        v_new_id := NULL;
        BEGIN
            EXECUTE format(
              'INSERT INTO %I.product (product_id, product_name, list_price, rowguid, modifieddate) VALUES ($1,$2,$3,$4,$5) RETURNING product_id',
              v_target_schema
            )
            USING rec.product_id, rec.product_name, rec.list_price, gen_random_uuid(), NOW()
            INTO v_new_id;
            v_products := v_products + 1;
        EXCEPTION WHEN OTHERS THEN
            BEGIN
                EXECUTE format(
                  'INSERT INTO %I.product (product_name, list_price, rowguid, modifieddate) VALUES ($1,$2,$3,$4) RETURNING product_id',
                  v_target_schema
                )
                USING rec.product_name, rec.list_price, gen_random_uuid(), NOW()
                INTO v_new_id;
                v_products := v_products + 1;
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Product insert into % failed: %', v_target_schema || '.product', SQLERRM;
                RAISE;
            END;
        END;

        INSERT INTO tmp_product_map(orig_product_key, orig_product_id, new_product_id) VALUES (rec.product_key, rec.product_id,
                                                                                               v_new_id);
    END LOOP;

    FOR rec IN
      SELECT DISTINCT dcat.category_key, dcat.category_id, dcat.category_name
      FROM dwh.dim_category dcat
      JOIN dwh.bridge_product_category bc ON bc.category_key = dcat.category_key
      JOIN dwh.dim_product dp ON dp.product_key = bc.product_key
      WHERE dcat.branch_key = v_branch_key AND dp.branch_key = v_branch_key
    LOOP
        v_new_id := NULL;
        BEGIN
            EXECUTE format('SELECT category_id FROM %I.category WHERE category_name = $1 LIMIT 1', v_target_schema)
            USING rec.category_name INTO v_new_id;
        EXCEPTION WHEN OTHERS THEN
            v_new_id := NULL;
        END;

        IF v_new_id IS NULL THEN
            BEGIN
                EXECUTE format(
                  'INSERT INTO %I.category (category_id, category_name, rowguid, modifieddate) VALUES ($1,$2,$3,$4) RETURNING category_id',
                  v_target_schema
                )
                USING rec.category_id, rec.category_name, gen_random_uuid(), NOW()
                INTO v_new_id;
                v_categories := v_categories + 1;
            EXCEPTION WHEN OTHERS THEN
                BEGIN
                    EXECUTE format(
                      'INSERT INTO %I.category (category_name, rowguid, modifieddate) VALUES ($1,$2,$3) RETURNING category_id',
                      v_target_schema
                    )
                    USING rec.category_name, gen_random_uuid(), NOW()
                    INTO v_new_id;
                    v_categories := v_categories + 1;
                EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE 'Category insert into % failed: %', v_target_schema || '.category', SQLERRM;
                    RAISE;
                END;
            END;
        END IF;

        INSERT INTO tmp_category_map(orig_category_key, orig_category_id, new_category_id) VALUES (rec.category_key, rec.category_id,
                                                                                                   v_new_id);
    END LOOP;


    FOR rec IN
      SELECT DISTINCT dp.product_key AS orig_product_key, dcat.category_key AS orig_category_key
      FROM dwh.bridge_product_category bc
      JOIN dwh.dim_product dp ON bc.product_key = dp.product_key
      JOIN dwh.dim_category dcat ON bc.category_key = dcat.category_key
      WHERE dp.branch_key = v_branch_key AND dcat.branch_key = v_branch_key
    LOOP
        SELECT new_product_id INTO v_new_prod FROM tmp_product_map WHERE orig_product_key = rec.orig_product_key LIMIT 1;
        SELECT new_category_id INTO v_new_id FROM tmp_category_map WHERE orig_category_key = rec.orig_category_key LIMIT 1;
        IF v_new_prod IS NOT NULL AND v_new_id IS NOT NULL THEN
            BEGIN
                EXECUTE format('INSERT INTO %I.product_category (product_id, category_id, rowguid, modifieddate) VALUES ($1,$2,$3,$4)', v_target_schema)
                USING v_new_prod, v_new_id, gen_random_uuid(), NOW();
            EXCEPTION WHEN unique_violation THEN
                NULL;
            END;
        END IF;
    END LOOP;

    FOR rec IN
      SELECT f.sale_id AS orig_sale_id, dd.full_date AS sale_date, dc.customer_key AS orig_customer_key, dc.customer_id AS orig_customer_id, SUM(f.line_amount)
          AS total_amount
      FROM dwh.fact_sale_item f
      JOIN dwh.dim_date dd ON f.date_key = dd.date_key
      JOIN dwh.dim_customer dc ON f.customer_key = dc.customer_key
      WHERE f.branch_key = v_branch_key
        AND dd.full_date BETWEEN p_start_date AND p_end_date
      GROUP BY f.sale_id, dd.full_date, dc.customer_key, dc.customer_id
      ORDER BY dd.full_date, f.sale_id
    LOOP
        SELECT new_customer_id INTO v_new_cust FROM tmp_customer_map WHERE (orig_customer_id = rec.orig_customer_id) OR (orig_customer_key = rec.orig_customer_key)
                                                                     LIMIT 1;
        IF v_new_cust IS NULL THEN
            RAISE EXCEPTION 'Customer mapping missing for source customer (id=% , key=%)', rec.orig_customer_id, rec.orig_customer_key;
        END IF;

        v_new_sale := NULL;
        BEGIN
            EXECUTE format('INSERT INTO %I.sale (sale_id, customer_id, sale_date, total_amount, rowguid, modifieddate) VALUES ($1,$2,$3,$4,$5,$6)', v_target_schema)
            USING rec.orig_sale_id, v_new_cust, rec.sale_date, rec.total_amount, gen_random_uuid(), NOW();
            v_new_sale := rec.orig_sale_id;
            v_sales := v_sales + 1;
        EXCEPTION WHEN OTHERS THEN
            BEGIN
                EXECUTE format('INSERT INTO %I.sale (customer_id, sale_date, total_amount, rowguid, modifieddate) VALUES ($1,$2,$3,$4,$5)', v_target_schema)
                USING v_new_cust, rec.sale_date, rec.total_amount, gen_random_uuid(), NOW();

                EXECUTE format('SELECT sale_id FROM %I.sale WHERE customer_id = $1 AND sale_date = $2 AND total_amount = $3 ORDER BY sale_id DESC LIMIT 1',
                               v_target_schema)
                USING v_new_cust, rec.sale_date, rec.total_amount INTO v_new_sale;

                IF v_new_sale IS NULL THEN
                    RAISE EXCEPTION 'Failed to retrieve inserted sale_id for customer % on date %', v_new_cust, rec.sale_date;
                END IF;
                v_sales := v_sales + 1;
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Sale insert into % failed: %', v_target_schema || '.sale', SQLERRM;
                RAISE;
            END;
        END;

        INSERT INTO tmp_sale_map(orig_sale_id, new_sale_id) VALUES (rec.orig_sale_id, v_new_sale);
    END LOOP;

    FOR rec IN
      SELECT f.sale_item_id AS orig_sale_item_id, f.sale_id AS orig_sale_id, f.product_key AS prod_key, f.quantity, f.unit_price, f.line_amount
      FROM dwh.fact_sale_item f
      JOIN dwh.dim_date dd ON f.date_key = dd.date_key
      WHERE f.branch_key = v_branch_key
        AND dd.full_date BETWEEN p_start_date AND p_end_date
      ORDER BY f.sale_id, f.sale_item_id
    LOOP
        ELECT new_product_id INTO v_new_prod FROM tmp_product_map WHERE orig_product_key = rec.prod_key LIMIT 1;
        IF v_new_prod IS NULL THEN
            RAISE EXCEPTION 'Product mapping missing for source product_key %', rec.prod_key;
        END IF;

        SELECT new_sale_id INTO v_new_sale FROM tmp_sale_map WHERE orig_sale_id = rec.orig_sale_id LIMIT 1;
        IF v_new_sale IS NULL THEN
            RAISE EXCEPTION 'Sale mapping missing for source sale %', rec.orig_sale_id;
        END IF;

        BEGIN
            EXECUTE format('INSERT INTO %I.sale_item (sale_item_id, sale_id, product_id, quantity, unit_price, line_amount, rowguid, modifieddate) ' ||
                           'VALUES ($1,$2,$3,$4,$5,$6,$7,$8)', v_target_schema)
            USING rec.orig_sale_item_id, v_new_sale, v_new_prod, rec.quantity, rec.unit_price, rec.line_amount, gen_random_uuid(), NOW();
            v_sale_items := v_sale_items + 1;
        EXCEPTION WHEN OTHERS THEN
            BEGIN
                EXECUTE format('INSERT INTO %I.sale_item (sale_id, product_id, quantity, unit_price, line_amount, rowguid, modifieddate) ' ||
                               'VALUES ($1,$2,$3,$4,$5,$6,$7)', v_target_schema)
                USING v_new_sale, v_new_prod, rec.quantity, rec.unit_price, rec.line_amount, gen_random_uuid(), NOW();
                v_sale_items := v_sale_items + 1;
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Sale_item insert into % failed: %', v_target_schema || '.sale_item', SQLERRM;
                RAISE;
            END;
        END;
    END LOOP;

    INSERT INTO dwh.restore_log(branch_code, start_date, end_date, customers_inserted, products_inserted, categories_inserted, sales_inserted, sale_items_inserted)
    VALUES (p_branch_code, p_start_date, p_end_date, v_customers, v_products,
            v_categories, v_sales, v_sale_items);

    RAISE NOTICE 'Restore completed for branch % between % and % (target schema: %). Inserted: customers=% products=% categories=% sales=% sale_items=%',
        p_branch_code, p_start_date, p_end_date, v_target_schema, v_customers, v_products, v_categories, v_sales, v_sale_items;

END;
$$;

