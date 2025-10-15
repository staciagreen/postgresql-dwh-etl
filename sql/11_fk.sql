\connect dwh

ALTER TABLE bridge_product_category
  ADD CONSTRAINT fk_bpc_product FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
  ADD CONSTRAINT fk_bpc_category FOREIGN KEY (category_sk) REFERENCES dim_category(category_sk);

ALTER TABLE fact_sale_item
  ADD CONSTRAINT fk_fact_customer FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
  ADD CONSTRAINT fk_fact_product  FOREIGN KEY (product_sk)   REFERENCES dim_product(product_sk),
  ADD CONSTRAINT fk_fact_date     FOREIGN KEY (date_sk)      REFERENCES dim_date(date_sk);

CREATE INDEX IF NOT EXISTS ix_fact_date    ON fact_sale_item(date_sk);
CREATE INDEX IF NOT EXISTS ix_fact_product ON fact_sale_item(product_sk);
CREATE INDEX IF NOT EXISTS ix_fact_cust    ON fact_sale_item(customer_sk);
