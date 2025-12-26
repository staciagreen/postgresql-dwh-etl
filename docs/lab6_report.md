
<div align="center">

<h3>Федеральное государственное автономное образовательное учреждение высшего образования</h3>
<h2>Университет ИТМО</h2>

<br/>

<h2>Лабораторная работа №6</h2>
<h3>Восстановление данных филиала из DWH</h3>

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
</div>

<div style="page-break-after: always;"></div>


# 1. Цель работы

Разработать и протестировать процедуру/скрипт восстановления утраченных данных филиала (branch) из центрального Хранилища данных (DWH). Процедура должна уметь восстановить справочники (customers, products, categories), связи product↔category, шапки продаж (sale) и строки продаж (sale_item) за указанный период, не нарушая целостность и не создавая дубликатов при повторном запуске.

---

# 2. Условие

Исходные данные хранятся в DWH (`dwh`): таблицы измерений (`dwh.dim_*`) и факт `dwh.fact_sale_item`. Цель — реализовать механизм восстановления утерянных данных в базе филиала (например, `branch_west`) по данным из DWH за заданный диапазон дат. Варианты реализации:

- хранимая процедура в DWH, которая через FDW выполняет вставки в удалённую схему `src_<branch>`; или
- CLI-скрипт, который читает из DWH и вставляет в базу филиала напрямую (использует psql/connection к `branch_<branch>`). 

Требования:
- избежать дублирования при повторном запуске (идемпотентность);
- в лог писать количество вставленных записей (таблица `dwh.restore_log` либо отдельный лог-файл);
- корректно маппить идентификаторы источников (customer_id, product_id, category_id) на целевые идентификаторы филиала.

---

# 3. Модель и подход

- Источник: `dwh.dim_customer`, `dwh.dim_product`, `dwh.dim_category`, `dwh.bridge_product_category`, `dwh.fact_sale_item`, `dwh.dim_date`, `dwh.dim_branch`.
- Цель: восстановить структуру публичной схемы филиала (`public` в базе `branch_<code>`) — таблицы `customer`, `product`, `category`, `product_category`, `sale`, `sale_item`.

Подходы:
- При восстановлении справочников — найти по естественному ключу (customer_name, product_name+list_price, category_name). Если не найдено — вставить. Сохранить mapping orig_id -> new_id.
- При восстановлении продаж: создать шапку `sale` (используя mapped customer_id), сохранить mapping orig_sale_id -> new_sale_id, затем вставить `sale_item` с новым `sale_id` и mapped product_id.
- Логирование: запись в `dwh.restore_log` (если процедура выполняется в DWH) или отдельный лог-файл `/tmp/restore_branch_<branch>_<from>_<to>.log` (при CLI).

---

# 4. Реализация

В репозитории реализованы две утилиты:

- `sql/60_restore_branch_from_dwh.sql` — процедура `restore_branch_from_dwh(p_branch_code text, p_start_date date, p_end_date date, p_force boolean default false)` в схеме `dwh`. Процедура создаёт временные таблицы mapping, вставляет справочники/продукты/категории/шапки/строки и логирует результаты в `dwh.restore_log`.
Ниже приведён пример вызова процедуры и ключевые фрагменты логики (сокращённо).

Пример вызова процедуры (psql):

```sql
-- В DWH (если процедура установлена):
CALL restore_branch_from_dwh('west'::text, '2025-01-02'::date, '2025-01-02'::date, true::boolean);

-- Проверить лог восстановления в DWH (если процедура логирует туда):
SELECT id, branch_code, start_date, end_date, customers_inserted, products_inserted, sales_inserted, sale_items_inserted
FROM dwh.restore_log ORDER BY id DESC LIMIT 10;
```

Если выполнение процедуры через FDW даёт ошибки (например, ограничение вставки в GENERATED ALWAYS identity), используйте CLI-скрипт:

```bash
# Запуск CLI-restore (вставляет данные напрямую в branch_west)
chmod +x scripts/restore_branch_from_dwh_cli.sh
./scripts/restore_branch_from_dwh_cli.sh west 2025-01-02 2025-01-02
# Лог: /tmp/restore_branch_west_2025-01-02_2025-01-02.log
```

Фрагмент логики (концептуально):

- Вставка customer (по customer_name) — если не найден, INSERT RETURNING customer_id; сохранить mapping.
- Вставка category (по category_name).
- Вставка product (по product_name + list_price).
- Вставка связей product_category (по новым id).
- Вставка sale (использовать mapped customer_id) — сохранить new sale_id.
- Вставка sale_item (использовать mapped product_id и mapped sale_id).
- Запись в dwh.restore_log или лог-файл с подсчётом вставленных строк.

---

# 5. Порядок запуска (чистый сценарий)

0) Полный старт с нуля
``` powershell
docker compose down -v
docker compose up -d --build
docker compose logs -f seeder
```

Дождись [OK] Seeded branch_west и [OK] Seeded branch_east, выйди Ctrl+C.

1) Поднять DWH
``` powershell
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/20_dwh_schema.sql
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/21_fdw.sql
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/22_dwh_load.sql
```

Проверка, что факт есть:
``` powershell
docker compose exec -T db psql -U postgres -d dwh -c "SELECT COUNT(*) AS fact_cnt FROM dwh.fact_sale_item;"
```
2) включить pgcrypto в dwh
``` powershell
docker compose exec -T db psql -U postgres -d dwh -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
```
3) Загрузить скрипт с процедурой в dwh
``` powershell
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/60_restore_branch_from_dwh.sql
```

Проверить, что процедура появилась:
``` powershell
docker compose exec -T db psql -U postgres -d dwh -c "\df+ restore_branch_from_dwh"
```

4.1. До очистки: показать, что branch_west не пустая
``` powershell
docker compose exec -T db psql -U postgres -d branch_west -c "
SELECT
  (SELECT COUNT(*) FROM customer) AS customers,
  (SELECT COUNT(*) FROM product)  AS products,
  (SELECT COUNT(*) FROM sale)     AS sales,
  (SELECT COUNT(*) FROM sale_item) AS sale_items;
"
```
4.2. Очистить branch_west (цель восстановления)
``` powershell
docker compose exec -T db psql -U postgres -d branch_west -c "
TRUNCATE sale_item, sale, product_category, product, category, customer RESTART IDENTITY;
"
```

Проверка, что пусто:
``` powershell
docker compose exec -T db psql -U postgres -d branch_west -c "
SELECT
  (SELECT COUNT(*) FROM customer) AS customers,
  (SELECT COUNT(*) FROM product)  AS products,
  (SELECT COUNT(*) FROM sale)     AS sales,
  (SELECT COUNT(*) FROM sale_item) AS sale_items;
"
```
5) Вызвать восстановление из DWH в ветку west
``` powershell
docker compose exec -T db psql -U postgres -d dwh -c \
"CALL restore_branch_from_dwh('west','2025-01-01','2025-11-19');"
```
6) Демонстрация корректности
``` powershell
6.1. Ветка west теперь снова не пустая
docker compose exec -T db psql -U postgres -d branch_west -c "
SELECT
  (SELECT COUNT(*) FROM customer) AS customers,
  (SELECT COUNT(*) FROM product)  AS products,
  (SELECT COUNT(*) FROM category) AS categories,
  (SELECT COUNT(*) FROM product_category) AS product_categories,
  (SELECT COUNT(*) FROM sale)     AS sales,
  (SELECT COUNT(*) FROM sale_item) AS sale_items;
"
```
6.2. В dwh появился лог восстановления
``` powershell
docker compose exec -T db psql -U postgres -d dwh -c "
SELECT *
FROM dwh.restore_log
ORDER BY id DESC
LIMIT 5;
"
```
6.3. Сравнить сумму продаж за период (DWH vs восстановленная ветка)

Сумма по DWH для west за период:
``` powershell
docker compose exec -T db psql -U postgres -d dwh -c "
SELECT SUM(f.line_amount) AS dwh_sum
FROM dwh.fact_sale_item f
JOIN dwh.dim_date dd ON dd.date_key=f.date_key
JOIN dwh.dim_branch b ON b.branch_key=f.branch_key
WHERE b.branch_code='west'
  AND dd.full_date BETWEEN '2025-01-01' AND '2025-11-19';
"
```

Сумма по восстановленной ветке branch_west за тот же период:
``` powershell
docker compose exec -T db psql -U postgres -d branch_west -c "
SELECT SUM(si.line_amount) AS branch_sum
FROM sale_item si
JOIN sale s ON s.sale_id=si.sale_id
WHERE s.sale_date BETWEEN '2025-01-01' AND '2025-11-19';
"
```
