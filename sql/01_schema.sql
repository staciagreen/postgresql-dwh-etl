-- 01_schema.sql
-- PostgreSQL. Нужен pgcrypto для gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Покупатели
CREATE TABLE IF NOT EXISTS customer (
    customer_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_name TEXT NOT NULL,
    rowguid       UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2) Категории
CREATE TABLE IF NOT EXISTS category (
    category_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_name TEXT NOT NULL UNIQUE,
    rowguid       UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3) Товары
CREATE TABLE IF NOT EXISTS product (
    product_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_name TEXT NOT NULL,
    list_price   NUMERIC(12,2) NOT NULL CHECK (list_price >= 0),
    rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3a) Связь M:N Товар–Категория
CREATE TABLE IF NOT EXISTS product_category (
    product_id   BIGINT NOT NULL,
    category_id  BIGINT NOT NULL,
    rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_product_category PRIMARY KEY (product_id, category_id)
);

-- 4) Сделки (шапка)
CREATE TABLE IF NOT EXISTS sale (
    sale_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  BIGINT NOT NULL,
    sale_date    DATE NOT NULL,
    total_amount NUMERIC(14,2) NOT NULL CHECK (total_amount >= 0),
    rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4a) Позиции сделки (детали)
CREATE TABLE IF NOT EXISTS sale_item (
    sale_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sale_id      BIGINT NOT NULL,
    product_id   BIGINT NOT NULL,
    quantity     NUMERIC(12,3) NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    line_amount  NUMERIC(14,2) NOT NULL CHECK (line_amount >= 0),
    rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
