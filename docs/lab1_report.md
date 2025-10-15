<div align="center">

<h3>Федеральное государственное автономное образовательное учреждение высшего образования</h3>
<h2>Университет ИТМО</h2>

<br/>

<h2>Лабораторная работа №1</h2>
<h3>Филиалы: схема и наполнение базы данных</h3>

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

<b>Санкт-Петербург</b><br/>
2025

</div>

<div style="page-break-after: always;"></div>

## Цель

Сформировать минимальную модель данных для филиала и развернуть её в двух БД с тестовым наполнением.

## Модель данных

![ER-диаграмма](img/data_model.png)

## Скрипты

- `sql/01_schema.sql` - создание таблиц

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS customer (
    customer_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_name TEXT NOT NULL,
    rowguid       UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS category (
    category_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_name TEXT NOT NULL UNIQUE,
    rowguid       UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS product (
    product_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_name TEXT NOT NULL,
    list_price   NUMERIC(12,2) NOT NULL CHECK (list_price >= 0),
    rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS product_category (
    product_id   BIGINT NOT NULL,
    category_id  BIGINT NOT NULL,
    rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_product_category PRIMARY KEY (product_id, category_id)
);

CREATE TABLE IF NOT EXISTS sale (
    sale_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  BIGINT NOT NULL,
    sale_date    DATE NOT NULL,
    total_amount NUMERIC(14,2) NOT NULL CHECK (total_amount >= 0),
    rowguid      UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

```

- `sql/02_fk.sql` - внешние ключи

```sql
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

```

- Docker init: `docker/init/00_full_init.sql`

```sql
CREATE DATABASE branch_west;
CREATE DATABASE branch_east;

\connect branch_west
\i /docker/sql/01_schema.sql
\i /docker/sql/02_fk.sql

\connect branch_east
\i /docker/sql/01_schema.sql
\i /docker/sql/02_fk.sql

```

- Сидер: `docker/seeder/seed.py`

```python
import os
import random
from datetime import date, timedelta
import psycopg2
from faker import Faker

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "postgres")
DBS = [db.strip() for db in os.getenv("DBS", "branch_west").split(",")]

fake = Faker("ru_RU")
random.seed(42)

CATEGORIES = [
    "Бытовая техника", "Электроника", "Одежда",
    "Спорттовары", "Книги", "Игрушки"
]

def connect(dbname):
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=dbname
    )

def seed_db(dbname):
    conn = connect(dbname)
    conn.autocommit = True
    cur = conn.cursor()

    for t in ["sale_item","sale","product_category","product","category","customer"]:
        cur.execute(f"DELETE FROM {t};")

    customers = []
    for _ in range(30):
        name = fake.name()
        cur.execute(
            "INSERT INTO customer(customer_name) VALUES (%s) RETURNING customer_id;",
            (name,)
        )
        customers.append(cur.fetchone()[0])

    categories = []
    for name in CATEGORIES:
        cur.execute(
            "INSERT INTO category(category_name) VALUES (%s) ON CONFLICT (category_name) DO NOTHING RETURNING category_id;",
            (name,)
        )
        row = cur.fetchone()
        if row is None:
            cur.execute("SELECT category_id FROM category WHERE category_name=%s;", (name,))
            row = cur.fetchone()
        categories.append(row[0])

    products = []
    for i in range(30):
        pname = f"{fake.word().capitalize()} {fake.color_name()}"
        price = round(random.uniform(5, 500), 2)
        cur.execute(
            "INSERT INTO product(product_name, list_price) VALUES (%s, %s) RETURNING product_id;",
            (pname, price)
        )
        products.append(cur.fetchone()[0])

    pairs = set()
    while len(pairs) < 60:
        pairs.add((random.choice(products), random.choice(categories)))
    for p, c in pairs:
        cur.execute(
            "INSERT INTO product_category(product_id, category_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (p, c)
        )

    sales = []
    for _ in range(50):
        cust = random.choice(customers)
        day = date.today() - timedelta(days=random.randint(0, 180))
        cur.execute(
            "INSERT INTO sale(customer_id, sale_date, total_amount) VALUES (%s, %s, 0) RETURNING sale_id;",
            (cust, day)
        )
        sales.append(cur.fetchone()[0])

    for s in sales:
        n_items = random.randint(1, 5)
        total = 0
        used = set()
        for _ in range(n_items):
            prod = random.choice(products)
            if prod in used:
                continue
            used.add(prod)
            qty = round(random.uniform(1, 5), 2)
            cur.execute("SELECT list_price FROM product WHERE product_id=%s;", (prod,))
            unit_price = float(cur.fetchone()[0])
            amount = round(qty * unit_price, 2)
            total += amount
            cur.execute(
                """INSERT INTO sale_item(sale_id, product_id, quantity, unit_price, line_amount)
                   VALUES (%s,%s,%s,%s,%s);""",
                (s, prod, qty, unit_price, amount)
            )
        cur.execute("UPDATE sale SET total_amount=%s WHERE sale_id=%s;", (round(total, 2), s))

    cur.close()
    conn.close()
    print(f"[OK] Seeded {dbname}")

def main():
    for db in DBS:
        seed_db(db)

if __name__ == "__main__":
    main()
```

## Развёртывание

```bash
docker compose down -v
docker compose up -d --build
```

## Результат заполнения
![im](img/branch_west_fill.png)

![im](img/branch_east_fill.png)

## Демонстрация целостности связей 
```powershell
docker compose exec -T db psql -U postgres -d branch_west -c "
INSERT INTO sale(customer_id, sale_date, total_amount) VALUES (999999,'2025-01-01',0);"
```
![im](img/целостность.png)
FK добавлены отдельными ALTER, поэтому нельзя создать продажу на несуществующего клиента.

## Деталь чека - ассоциативная сущность M:N с атрибутами
``` powershell
docker compose exec -T db psql -U postgres -d branch_west -c "
SELECT s.sale_id, to_char(s.sale_date,'YYYY-MM-DD') d, p.product_name,
       si.quantity, si.unit_price, si.line_amount
FROM sale_item si
JOIN sale s    ON s.sale_id = si.sale_id
JOIN product p ON p.product_id = si.product_id
ORDER BY s.sale_id, p.product_id
LIMIT 5;"
```
![im](img/sale_detail.png)
sale_item связывает sale и product + хранит quantity, unit_price, line_amount
## rowguid и ModifiedDate
```powershell
docker compose exec -T db psql -U postgres -d branch_west -c "
SELECT customer_id, customer_name, rowguid, ModifiedDate
FROM customer
ORDER BY customer_id
LIMIT 3;"
```
![im](img/rm.png)
