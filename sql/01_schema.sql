-- Подключаем расширение pgcrypto для функции gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Таблица customer: покупатели филиала
CREATE TABLE IF NOT EXISTS customer (
    -- Суррогатный первичный ключ
    customer_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Имя покупателя (или название организации)
    customer_name TEXT NOT NULL,
    -- Глобальный идентификатор записи для межсистемной синхронизации
    rowguid       UUID NOT NULL DEFAULT gen_random_uuid(),
    -- Время добавления или последнего изменения записи
    ModifiedDate  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Таблица category: категории товаров
CREATE TABLE IF NOT EXISTS category (
    -- Суррогатный первичный ключ
    category_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Название категории (уникально)
    category_name TEXT NOT NULL UNIQUE,
    -- Глобальный идентификатор записи
    rowguid       UUID NOT NULL DEFAULT gen_random_uuid(),
    -- Время добавления или последнего изменения записи
    ModifiedDate  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Таблица product: справочник товаров
CREATE TABLE IF NOT EXISTS product (
    -- Суррогатный первичный ключ
    product_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Название товара
    product_name TEXT NOT NULL,
    -- Каталожная цена (точный десятичный тип: 12 цифр всего, 2 после запятой)
    list_price   NUMERIC(12,2) NOT NULL CHECK (list_price >= 0),
    -- Глобальный идентификатор записи
    rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
    -- Время добавления или последнего изменения записи
    ModifiedDate TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Таблица product_category: связь M:N между product и category
CREATE TABLE IF NOT EXISTS product_category (
    -- Ссылка на товар
    product_id   BIGINT NOT NULL,
    -- Ссылка на категорию
    category_id  BIGINT NOT NULL,
    -- Глобальный идентификатор записи (для трассировки изменений, не ключ)
    rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
    -- Время добавления или последнего изменения записи
    ModifiedDate TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Составной первичный ключ исключает дублирование пар товар-категория
    CONSTRAINT pk_product_category PRIMARY KEY (product_id, category_id)
);

-- Таблица sale: шапка продажи
CREATE TABLE IF NOT EXISTS sale (
    -- Суррогатный первичный ключ
    sale_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Покупатель, совершивший сделку
    customer_id  BIGINT NOT NULL,
    -- Дата продажи (без времени)
    sale_date    DATE NOT NULL,
    -- Итоговая сумма по документу продажи
    total_amount NUMERIC(14,2) NOT NULL CHECK (total_amount >= 0),
    -- Глобальный идентификатор записи
    rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
    -- Время добавления или последнего изменения записи
    ModifiedDate TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Таблица sale_item: строки продажи (позиции)
CREATE TABLE IF NOT EXISTS sale_item (
    -- Суррогатный первичный ключ строки
    sale_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Ссылка на документ продажи
    sale_id      BIGINT NOT NULL,
    -- Ссылка на товар
    product_id   BIGINT NOT NULL,
    -- Количество товара (дробное, если требуется)
    quantity     NUMERIC(12,3) NOT NULL CHECK (quantity > 0),
    -- Фактическая цена за единицу в момент продажи
    unit_price   NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    -- Сумма по строке: quantity * unit_price
    line_amount  NUMERIC(14,2) NOT NULL CHECK (line_amount >= 0),
    -- Глобальный идентификатор записи
    rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
    -- Время добавления или последнего изменения записи
    ModifiedDate TIMESTAMPTZ NOT NULL DEFAULT NOW()
);