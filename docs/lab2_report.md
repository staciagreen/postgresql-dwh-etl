
__**********<div align="center">

### Федеральное государственное автономное образовательное учреждение высшего образования  
## Университет ИТМО

<br/>

# Лабораторная работа №2  
### Разработка модели и скрипта создания хранилища данных

<br/><br/>

<table>
  <tr>
    <td align="right"><b>Дисциплина:</b></td>
    <td align="left">Технологии управления данными</td>
  </tr>
  <tr>
    <td align="right"><b>Группа:</b></td>
    <td align="left">M3307</td>
  </tr>
  <tr>
    <td align="right"><b>Студент:</b></td>
    <td align="left">Гринько Анастасия Павловна</td>
  </tr>
  <tr>
    <td align="right"><b>Преподаватель:</b></td>
    <td align="left">Повышев Владислав Вячеславович</td>
  </tr>
</table>

<br/><br/><br/>

**Санкт-Петербург**  
2025

</div>

---

## 1. Условие лабораторной работы

Создать реляционное хранилище данных (DWH) на основе данных из двух филиалов (branch_west, branch_east).  
Выбрать и обосновать архитектуру (Кимбалл / Инмон / Якорная / Data Vault и т.п.).  
Разработать скрипт создания DWH и тестовый скрипт загрузки из источников ЛР‑1 в *автоматизированном режиме*.  
Для DWH допускается только добавление (insert-only): операции модификации/удаления данных не выполняются.

Источники - уже засеянные БД ЛР‑1. Подключение к ним выполняется из БД dwh через postgres_fdw.

## 2. Выбор и обоснование архитектуры

Выбрана **звезда (Кимбалл)** в духе базового учебного примера: один факт и набор плоских измерений с суррогатными ключами. Причины:
- простота схемы и воспроизводимости (понятная защита);
- быстрые аналитические запросы (джойним факт с измерениями по *_sk);
- независимость DWH от внутренних идентификаторов источников (идём через собственные *_sk);

## 3. Модель DWH (логика и таблицы)
![Схема](img/kimball.png)
**Факт:** `dwh.fact_sale_item`  - позиции продаж.  
**Измерения:**  
- dwh.dim_branch  - код филиала (branch_code = 'west'|'east');  
- dwh.dim_date  - календарь (full_date, year, month, day, week, isodow);  
- dwh.dim_customer  - покупатели (branch_sk, customer_id, customer_name);  
- dwh.dim_product  - товары (branch_sk, product_id, product_name, list_price);  
- dwh.dim_category  - категории (branch_sk, category_id, category_name);  
**Мост:** dwh.bridge_product_category  - связь товар<->категория (M:N).

**Ключи и ссылки:**
- Во всех таблицах применяются **суррогатные ключи** *_sk ( SERIAL).  
- Для избежания коллизий одинаковых *_id из разных филиалов, в измерениях присутствует **branch_sk**.  
- Во факте хранится sale_id и sale_item_id как атрибуты строки факта (идентификаторы позиции в источнике).  
- Загрузка  - **полная перезаливка**: на каждом прогоне очищаем факт/мост/измерения и заново заполняем из src_west/* и src_east/*.

## 4. Скрипты ЛР‑2 (перечень и места для вставки содержимого)

### 4.1. `20_dwh_schema.sql`  - создание схемы и таблиц DWH (простая звезда)
```sql
CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.dim_branch (
  branch_sk   SERIAL PRIMARY KEY,
  branch_code TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
  date_sk   SERIAL PRIMARY KEY,
  full_date DATE NOT NULL UNIQUE,
  year      INT  NOT NULL,
  month     INT  NOT NULL,
  day       INT  NOT NULL,
);
CREATE INDEX IF NOT EXISTS ix_dim_date_full_date ON dwh.dim_date(full_date);

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
  customer_sk    SERIAL PRIMARY KEY,
  branch_sk     BIGINT NOT NULL REFERENCES dwh.dim_branch(branch_sk),
  customer_id    BIGINT NOT NULL,
  customer_name  TEXT   NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_customer_src ON dwh.dim_customer(branch_sk, customer_id);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
  product_sk    SERIAL PRIMARY KEY,
  branch_sk    BIGINT NOT NULL REFERENCES dwh.dim_branch(branch_sk),
  product_id    BIGINT NOT NULL,
  product_name  TEXT   NOT NULL,
  list_price    NUMERIC(12,2) NOT NULL CHECK (list_price >= 0)
);
CREATE INDEX IF NOT EXISTS ix_product_src ON dwh.dim_product(branch_sk, product_id);

CREATE TABLE IF NOT EXISTS dwh.dim_category (
  category_sk    SERIAL PRIMARY KEY,
  branch_sk     BIGINT NOT NULL REFERENCES dwh.dim_branch(branch_sk),
  category_id    BIGINT NOT NULL,
  category_name  TEXT   NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_category_src ON dwh.dim_category(branch_sk, category_id);


CREATE TABLE IF NOT EXISTS dwh.bridge_product_category (
  product_sk  BIGINT NOT NULL REFERENCES dwh.dim_product(product_sk) ON DELETE CASCADE,
  category_sk BIGINT NOT NULL REFERENCES dwh.dim_category(category_sk) ON DELETE CASCADE,
  PRIMARY KEY (product_sk, category_sk)
);

CREATE TABLE IF NOT EXISTS dwh.fact_sale_item (
  fact_sk       SERIAL PRIMARY KEY,
  branch_sk    BIGINT NOT NULL REFERENCES dwh.dim_branch(branch_sk),
  date_sk      BIGINT NOT NULL REFERENCES dwh.dim_date(date_sk),
  customer_sk  BIGINT NOT NULL REFERENCES dwh.dim_customer(customer_sk),
  product_sk   BIGINT NOT NULL REFERENCES dwh.dim_product(product_sk),
  sale_id       BIGINT NOT NULL,
  sale_item_id  BIGINT NOT NULL,
  quantity      NUMERIC(12,3) NOT NULL CHECK (quantity > 0),
  unit_price    NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
  line_amount   NUMERIC(14,2) NOT NULL CHECK (line_amount >= 0)
);
CREATE TABLE IF NOT EXISTS dwh.fact_sale_item (
  fact_sk       SERIAL PRIMARY KEY,
  branch_sk     BIGINT NOT NULL REFERENCES dwh.dim_branch(branch_sk),
  date_sk       BIGINT NOT NULL REFERENCES dwh.dim_date(date_sk),
  customer_sk   BIGINT NOT NULL REFERENCES dwh.dim_customer(customer_sk),
  product_sk    BIGINT NOT NULL REFERENCES dwh.dim_product(product_sk),

  sale_id       BIGINT NOT NULL,
  sale_item_id  BIGINT NOT NULL,


  quantity      NUMERIC(12,3) NOT NULL CHECK (quantity > 0),
  unit_price    NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
  list_price    NUMERIC(12,2) NOT NULL CHECK (list_price >= 0),
  line_amount   NUMERIC(14,2) NOT NULL CHECK (line_amount >= 0),

  CONSTRAINT uq_fact_sale_item_nat UNIQUE (branch_sk, sale_id, sale_item_id)
);

```

### 4.2. `21_fdw.sql`  - настройка FDW (импорт src_west/* и src_east/* в БД dwh)
```sql
\connect dwh;

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SERVER IF EXISTS srv_west CASCADE;
CREATE SERVER srv_west FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (dbname 'branch_west', host '/var/run/postgresql');

DROP SERVER IF EXISTS srv_east CASCADE;
CREATE SERVER srv_east FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (dbname 'branch_east', host '/var/run/postgresql');

CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER srv_west OPTIONS (user 'postgres');
CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER srv_east OPTIONS (user 'postgres');

CREATE SCHEMA IF NOT EXISTS src_west;
CREATE SCHEMA IF NOT EXISTS src_east;

IMPORT FOREIGN SCHEMA public LIMIT TO (customer, product, category, product_category, sale, sale_item)
  FROM SERVER srv_west INTO src_west;

IMPORT FOREIGN SCHEMA public LIMIT TO (customer, product, category, product_category, sale, sale_item)
  FROM SERVER srv_east INTO src_east;

```

### 4.3. `22_dwh_load.sql`  - полная перезагрузка DWH из филиалов
```sql
BEGIN;
INSERT INTO dwh.dim_branch(branch_code) VALUES ('west'), ('east');

WITH bounds AS (
  SELECT
    LEAST( (SELECT MIN(sale_date) FROM src_west.sale),
           (SELECT MIN(sale_date) FROM src_east.sale) ) AS dmin,
    GREATEST( (SELECT MAX(sale_date) FROM src_west.sale),
              (SELECT MAX(sale_date) FROM src_east.sale) ) AS dmax
)
INSERT INTO dwh.dim_date(full_date, year, month, day, week, isodow)
SELECT d::date,
       EXTRACT(YEAR FROM d)::int,
       EXTRACT(MONTH FROM d)::int,
       EXTRACT(DAY FROM d)::int,
       EXTRACT(WEEK FROM d)::int,
       EXTRACT(ISODOW FROM d)::int
FROM bounds b
CROSS JOIN generate_series(b.dmin, b.dmax, interval '1 day') AS g(d)
ORDER BY 1;

INSERT INTO dwh.dim_customer(branch_sk, customer_id, customer_name)
SELECT br.branch_sk, c.customer_id, c.customer_name
FROM src_west.customer c
JOIN dwh.dim_branch br ON br.branch_code='west'
UNION ALL
SELECT br.branch_sk, c.customer_id, c.customer_name
FROM src_east.customer c
JOIN dwh.dim_branch br ON br.branch_code='east';

INSERT INTO dwh.dim_product(branch_sk, product_id, product_name, list_price)
SELECT br.branch_sk, p.product_id, p.product_name, p.list_price
FROM src_west.product p
JOIN dwh.dim_branch br ON br.branch_code='west'
UNION ALL
SELECT br.branch_sk, p.product_id, p.product_name, p.list_price
FROM src_east.product p
JOIN dwh.dim_branch br ON br.branch_code='east';

INSERT INTO dwh.dim_category(branch_sk, category_id, category_name)
SELECT br.branch_sk, c.category_id, c.category_name
FROM src_west.category c
JOIN dwh.dim_branch br ON br.branch_code='west'
UNION ALL
SELECT br.branch_sk, c.category_id, c.category_name
FROM src_east.category c
JOIN dwh.dim_branch br ON br.branch_code='east';

INSERT INTO dwh.bridge_product_category(product_sk, category_sk)
SELECT dp.product_sk, dc.category_sk
FROM src_west.product_category pc
JOIN dwh.dim_branch br ON br.branch_code='west'
JOIN dwh.dim_product  dp ON dp.branch_sk = br.branch_sk AND dp.product_id = pc.product_id
JOIN dwh.dim_category dc ON dc.branch_sk = br.branch_sk AND dc.category_id = pc.category_id
UNION ALL
SELECT dp.product_sk, dc.category_sk
FROM src_east.product_category pc
JOIN dwh.dim_branch br ON br.branch_code='east'
JOIN dwh.dim_product  dp ON dp.branch_sk = br.branch_sk AND dp.product_id = pc.product_id
JOIN dwh.dim_category dc ON dc.branch_sk = br.branch_sk AND dc.category_id = pc.category_id
ON CONFLICT DO NOTHING;

INSERT INTO dwh.fact_sale_item(
  branch_sk, date_sk, customer_sk, product_sk,
  sale_id, sale_item_id, quantity, unit_price, line_amount
)

SELECT br.branch_sk, dd.date_sk, dc.customer_sk, dp.product_sk,
       s.sale_id, si.sale_item_id, si.quantity, si.unit_price, si.line_amount
FROM src_west.sale_item si
JOIN src_west.sale s   ON s.sale_id = si.sale_id
JOIN dwh.dim_branch br ON br.branch_code = 'west'
JOIN dwh.dim_date   dd ON dd.full_date = s.sale_date
JOIN dwh.dim_customer dc ON dc.branch_sk = br.branch_sk AND dc.customer_id = s.customer_id
JOIN dwh.dim_product  dp ON dp.branch_sk = br.branch_sk AND dp.product_id = si.product_id

UNION ALL

SELECT br.branch_sk, dd.date_sk, dc.customer_sk, dp.product_sk,
       s.sale_id, si.sale_item_id, si.quantity, si.unit_price, si.line_amount
FROM src_east.sale_item si
JOIN src_east.sale s   ON s.sale_id = si.sale_id
JOIN dwh.dim_branch br ON br.branch_code = 'east'
JOIN dwh.dim_date   dd ON dd.full_date = s.sale_date
JOIN dwh.dim_customer dc ON dc.branch_sk = br.branch_sk AND dc.customer_id = s.customer_id
JOIN dwh.dim_product  dp ON dp.branch_sk = br.branch_sk AND dp.product_id = si.product_id;

COMMIT;

```

## 5. Как поднять

```powershell
docker compose down -v
docker compose up -d --build
Start-Sleep -Seconds 15
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/22_dwh_load.sql 
```

### 5.1. Проверить, что FDW импортировал исходники в DWH
```powershell
docker compose exec -T db psql -U postgres -d dwh -c "SELECT foreign_table_schema, foreign_table_name
   FROM information_schema.foreign_tables
  WHERE foreign_table_schema IN ('src_west','src_east')
  ORDER BY 1,2;"
  
  # через FDW из dwh
docker compose exec -T db psql -U postgres -d dwh -c "SELECT 'west' src, COUNT(*) FROM src_west.sale
 UNION ALL
 SELECT 'east', COUNT(*) FROM src_east.sale;"

```
![Схема](img/5.1.png)
### 5.2. Быстрая проверка, что источники не пустые
```powershell
docker compose exec -T db psql -U postgres -d dwh -c "SELECT 'west' AS src, COUNT(*) FROM src_west.sale
 UNION ALL
 SELECT 'east', COUNT(*) FROM src_east.sale;"
```
![Схема](img/5.2.png)

## 6. Проверки

### 6.1. Базовые объёмы (все таблицы DWH заполнены и не пустые)
```powershell
docker compose exec -T db psql -U postgres -d dwh -c "
SELECT 'dim_branch', COUNT(*) FROM dwh.dim_branch
UNION ALL SELECT 'dim_date', COUNT(*) FROM dwh.dim_date
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dwh.dim_customer
UNION ALL SELECT 'dim_product', COUNT(*) FROM dwh.dim_product
UNION ALL SELECT 'dim_category', COUNT(*) FROM dwh.dim_category
UNION ALL SELECT 'bridge_product_category', COUNT(*) FROM dwh.bridge_product_category
UNION ALL SELECT 'fact_sale_item', COUNT(*) FROM dwh.fact_sale_item;"
```
![Схема](img/6.1.png)

### 6.2. Целостность (FK): факты ссылаются на существующую дату
```powershell
docker compose exec -T db psql -U postgres -d dwh -c "
SELECT COUNT(*) AS broken_dates
FROM dwh.fact_sale_item f LEFT JOIN dwh.dim_date d ON d.date_sk = f.date_sk
WHERE d.date_sk IS NULL;"
```
![Схема](img/6.2.png)

### 6.3. Сверка сумм: DWH vs источники (через FDW)
```powershell
# сумма по DWH
docker compose exec -T db psql -U postgres -d dwh -c "
SELECT SUM(line_amount) AS dwh_total FROM dwh.fact_sale_item;"

# сумма по источникам
docker compose exec -T db psql -U postgres -d dwh -c "
SELECT SUM(line_amount) AS src_total
FROM (
  SELECT line_amount FROM src_west.sale_item
  UNION ALL
  SELECT line_amount FROM src_east.sale_item
) t;"
```

## 7. Пояснения к скриптам

### 7.1. `20_dwh_schema.sql`
- создаёт схему `dwh` и таблицы измерений (`dim_branch`, `dim_date`, `dim_customer`, `dim_product`, `dim_category`), мост `bridge_product_category` и факт `fact_sale_item`;  
- применяются **суррогатные ключи** `*_sk`, в измерениях присутствует `branch_sk` для отделения одинаковых `*_id` разных филиалов;  
- добавлены индексы для ускорения загрузки и аналитических запросов.

### 7.2. `21_fdw.sql`
- включает расширение `postgres_fdw`, объявляет сервера `srv_west` и `srv_east` и импортирует таблицы `public.*` из БД `branch_west` и `branch_east` в схемы `src_west` и `src_east` внутри БД `dwh`;  
- в init‑сценарии возможно использование Unix‑сокета (`host '/var/run/postgresql'`)  - это устраняет проблемы старта «через TCP» на ранней фазе контейнера.

### 7.3. `22_dwh_load.sql`
- строит календарь по диапазону дат продаж из обеих веток (`MIN/MAX` в `src_west.sale` и `src_east.sale`);  
- заполняет измерения, мост и факт простыми `INSERT ... SELECT` из `src_*`;  
- каждый повторный прогон даёт **полностью воспроизводимый** результат (полная перезагрузка).**********__
