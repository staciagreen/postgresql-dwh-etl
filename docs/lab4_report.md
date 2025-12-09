<div align="center">

### Федеральное государственное автономное образовательное учреждение высшего образования  
## Университет ИТМО

<br/>

# Лабораторная работа №4  
### Создание процедуры наполнения Хранилища данных

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

# 1. Цель работы

Цель работы - разработать хранимую процедуру или скрипт, обеспечивающий автоматизированный перенос данных из Филиалов Запад и Восток в центральное Хранилище данных (DWH).  
Процедура должна гарантировать отсутствие дублирования строк и минимальную нагрузку на исходные сервера филиалов.

---

# 2. Условие (дословно по методическим указаниям)

> **Лабораторная работа №4. Создания процедуры наполнения Хранилища данных.**
>
> Цель работы: создать хранимую процедуру, или скрипт, обеспечивающую автоматизированный перенос данных из Филиалов в Хранилище. Процедура должна обеспечить добавление вновь поступившей информации из Филиала Запад, и Филиала Восток в центральное хранилище данных. Процедура должна исключить дублирование информации. Разработчик должен продумать механизм обеспечивающий минимализацию нагрузки на продуктивные сервера филиалов. В процедуре можно использовать операции множественной вставки.

---

# 3. Теоретические сведения

В данной лабораторной работе реализуется **инкрементальная загрузка** (incremental load) в архитектуре *звезда (Kimball)*.  
Цели инкрементальной загрузки:

- не перезагружать исторические данные;
- выбирать только новые записи;
- минимизировать JOIN-операции по источникам (используются PK, surrogate keys);
- исключать дубликаты в фактах:  
  уникальность по `(branch_key, sale_id, sale_item_id)`.

Определение "последней загруженной даты" производится через:

```sql
SELECT MAX(full_date) FROM dwh.dim_date
JOIN dwh.fact_sale_item USING(date_key);
```

Все вставки происходят через **единичный запрос c CTE**, что снижает количество обращений к источникам через FDW.

---

# 4. Реализация

## 4.1. Скрипт `40_dwh_incremental_load.sql`

Ниже приведён итоговый рабочий скрипт, корректно загружающий только новые данные.

```sql
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
```


# 5. Порядок запуска
## Чистый запуск окружения

```bash
docker compose down -v
docker compose up -d --build
docker compose logs -f seeder
```
```bash
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/22_dwh_load.sql
docker compose exec -T db psql -U postgres -d dwh -c "SELECT COUNT(*) FROM dwh.fact_sale_item;"
```

## Запуск процедуры на пустых филиалах

```bash
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/40_dwh_incremental_load.sql
```

## Добавление нового чека в филиал West

Добавляем данные:

```powershell
docker compose exec -T db psql -U postgres -d branch_west -c "
INSERT INTO customer (customer_name)
VALUES ('Demo Customer 777')
RETURNING customer_id;
"
```
31
```powershell
docker compose exec -T db psql -U postgres -d branch_west -c "
INSERT INTO product (product_name, list_price)
VALUES ('Demo Product 777', 777) 
RETURNING product_id;
"
```
31
Создаём чек:

```powershell
docker compose exec -T db psql -U postgres -d branch_west -c "
INSERT INTO sale (customer_id, sale_date, total_amount)
VALUES (31, '2025-01-01', 777)
RETURNING sale_id;
"
```
51
Добавляем строку чека:

```powershell
docker compose exec -T db psql -U postgres -d branch_west -c "
INSERT INTO sale_item (sale_id, product_id, quantity, unit_price, line_amount)
VALUES (51, 31, 1, 999, 999)
RETURNING sale_item_id;
"
```

152

## Удаление другого чека в филиале

```powershell
docker compose exec -T db psql -U postgres -d branch_west -c "
DELETE FROM sale_item WHERE sale_id = 10;
DELETE FROM sale WHERE sale_id = 10;
"
```

## Повторный запуск процедуры переноса

```bash
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/40_dwh_incremental_load.sql
```


## Проверка загрузки нового чека

```bash
docker compose exec -T db psql -U postgres -d dwh -c "
SELECT sale_id, sale_item_id, line_amount
FROM dwh.fact_sale_item
WHERE sale_id = 51;
"
```

## Проверка, что удалённый чек сохраняется в DWH

```bash
docker compose exec -T db psql -U postgres -d dwh -c "
SELECT sale_id, sale_item_id
FROM dwh.fact_sale_item
WHERE sale_id = 10;
"
```

## Демонстрация идемпотентности
```bash
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/40_dwh_incremental_load.sql
```


