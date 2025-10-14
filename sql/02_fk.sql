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
