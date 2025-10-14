-- 02_fk_and_indexes.sql

-- Связи между таблицами
ALTER TABLE IF EXISTS product_category
  ADD CONSTRAINT fk_pc_product
  FOREIGN KEY (product_id) REFERENCES product(product_id);

ALTER TABLE IF EXISTS product_category
  ADD CONSTRAINT fk_pc_category
  FOREIGN KEY (category_id) REFERENCES category(category_id);

ALTER TABLE IF EXISTS sale
  ADD CONSTRAINT fk_sale_customer
  FOREIGN KEY (customer_id) REFERENCES customer(customer_id);

ALTER TABLE IF EXISTS sale_item
  ADD CONSTRAINT fk_sale_item_sale
  FOREIGN KEY (sale_id) REFERENCES sale(sale_id);

ALTER TABLE IF EXISTS sale_item
  ADD CONSTRAINT fk_sale_item_product
  FOREIGN KEY (product_id) REFERENCES product(product_id);

-- Индексы для улучшения производительности
CREATE INDEX IF NOT EXISTS ix_sale_date ON sale(sale_date);
CREATE INDEX IF NOT EXISTS ix_sale_customer ON sale(customer_id);
CREATE INDEX IF NOT EXISTS ix_si_sale ON sale_item(sale_id);
CREATE INDEX IF NOT EXISTS ix_si_product ON sale_item(product_id);
CREATE INDEX IF NOT EXISTS ix_pc_product ON product_category(product_id);
CREATE INDEX IF NOT EXISTS ix_pc_category ON product_category(category_id);
