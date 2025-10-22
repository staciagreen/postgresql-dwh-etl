
<div align="center">

### Федеральное государственное автономное образовательное учреждение высшего образования  
## Университет ИТМО

<br/>

# Лабораторная работа №3 
### 

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


## 1. Цель работы

Сконструировать витрину (data mart) для анализа продаж по неделям ISO с разрезом по филиалам: разработать схему витрины, реализовать скрипты создания и загрузки, выполнить проверку корректности данных и показать примерные аналитические запросы.

## 2. Условие

По методическим указаниям к ЛР‑3 требуется "консолидировать результаты продаж по неделям", выделив сущности **Недели** и **Продажи**. В качестве источника используется уже построенный DWH (ЛР‑2): факт продаж и календарь дат. Необходимо:
- спроектировать таблицы витрины;
- реализовать загрузку витрины из DWH;
- подготовить запросы для валидации и примеры аналитики.

## 3. Модель
![Схема](img/view.png)

## 4. Теоретические сведения

**ISO‑неделя** определяется номером недели iso_week и ISO‑годом iso_year`. Неделя начинается в понедельник (ISODOW=1) и заканчивается в воскресенье (ISODOW=7). Для привязки даты продажи к неделе используется календарь dwh.dim_date (поля full_date, isodow ).

В витрине выбраны следующие метрики на уровне агрегации "филиал × неделя":
- revenue_total - суммарная выручка;
- items_qty_total - суммарное количество позиций;
- orders_count - число чеков (заказов);
- customers_count - число уникальных покупателей;
- производные: avg_check = revenue_total / orders_count , avg_items_per_order = items_qty_total / orders_count .

## 5. Проектирование витрины

Будут созданы схема dm и две таблицы:

### 5.1. `dm.dim_week` - измерение "Неделя"
- week_key - суррогатный PK;  
- iso_year, iso_week - координаты ISO‑недели;  
- week_start_date, week_end_date - границы (пн…вс);  
- уникальность по (iso_year, iso_week).

### 5.2. `dm.fact_week_sales` - факт "Продажи по неделям"
- Гранулярность: **одна строка = филиал x неделя**;
- Ключи: branch_key  -> dwh.dim_branch, week_key  -> dm.dim_week ;
- Метрики: revenue_total, items_qty_total , orders_count , customers_count , avg_check , avg_items_per_order ;
- Уникальность по ( branch_key , week_key ).

Диаграмма уровня атрибутов соответствует классической витрине "звезда" (факт + малое измерение недели).

## 6. Реализация (SQL‑скрипты)

### 6.1.  `docker/sql/30_dm_schema.sql` - создание схемы и таблиц
```sql
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS dm.dim_week (
  week_key         BIGSERIAL PRIMARY KEY,
  iso_year         INT    NOT NULL,
  iso_week         INT    NOT NULL,
  week_start_date  DATE   NOT NULL,
  week_end_date    DATE   NOT NULL,
  CONSTRAINT uq_dim_week UNIQUE (iso_year, iso_week),
  CONSTRAINT ck_iso_week CHECK (iso_week BETWEEN 1 AND 53)
);

CREATE INDEX IF NOT EXISTS ix_dim_week_start ON dm.dim_week(week_start_date);
CREATE INDEX IF NOT EXISTS ix_dim_week_uniq  ON dm.dim_week(iso_year, iso_week);

CREATE TABLE IF NOT EXISTS dm.fact_week_sales (
  week_sales_key      BIGSERIAL PRIMARY KEY,
  branch_sk           BIGINT NOT NULL REFERENCES dwh.dim_branch(branch_sk),
  week_key            BIGINT NOT NULL REFERENCES dm.dim_week(week_key),

  revenue_total       NUMERIC(16,2) NOT NULL,
  items_qty_total     NUMERIC(16,3) NOT NULL,
  orders_count        INT           NOT NULL,
  customers_count     INT           NOT NULL,

  avg_check           NUMERIC(16,2) NOT NULL,
  avg_items_per_order NUMERIC(16,3) NOT NULL,

  CONSTRAINT uq_week_branch UNIQUE (branch_sk, week_key)
);

CREATE INDEX IF NOT EXISTS ix_week_sales_week ON dm.fact_week_sales(week_key);
CREATE INDEX IF NOT EXISTS ix_week_sales_br   ON dm.fact_week_sales(branch_sk);

```

### 6.2. `docker/sql/31_dm_load.sql` - полная загрузка витрины
```sql
BEGIN;

TRUNCATE dm.fact_week_sales, dm.dim_week RESTART IDENTITY;

WITH base AS (
  SELECT
    d.full_date,
    EXTRACT(ISOYEAR FROM d.full_date)::int AS iso_year,
    EXTRACT(WEEK    FROM d.full_date)::int AS iso_week,
    EXTRACT(ISODOW  FROM d.full_date)::int AS isodow
  FROM dwh.dim_date d
),
weeks AS (
  SELECT
    iso_year,
    iso_week,
    (MIN(full_date) - (MIN(isodow)-1) * INTERVAL '1 day')::date AS week_start_date,
    ((MIN(full_date) - (MIN(isodow)-1) * INTERVAL '1 day') + INTERVAL '6 day')::date AS week_end_date
  FROM base
  GROUP BY iso_year, iso_week
)
INSERT INTO dm.dim_week(iso_year, iso_week, week_start_date, week_end_date)
SELECT iso_year, iso_week, week_start_date, week_end_date
FROM weeks
ORDER BY iso_year, iso_week;

INSERT INTO dm.fact_week_sales(
  branch_sk, week_key,
  revenue_total, items_qty_total, orders_count, customers_count,
  avg_check, avg_items_per_order
)
SELECT
  f.branch_sk,
  w.week_key,
  SUM(f.line_amount)::numeric(16,2)          AS revenue_total,
  SUM(f.quantity)::numeric(16,3)             AS items_qty_total,
  COUNT(DISTINCT f.sale_id)                  AS orders_count,
  COUNT(DISTINCT f.customer_sk)              AS customers_count,
  CASE WHEN COUNT(DISTINCT f.sale_id)=0
       THEN 0::numeric(16,2)
       ELSE (SUM(f.line_amount) / COUNT(DISTINCT f.sale_id))::numeric(16,2)
  END                                        AS avg_check,
  CASE WHEN COUNT(DISTINCT f.sale_id)=0
       THEN 0::numeric(16,3)
       ELSE (SUM(f.quantity) / COUNT(DISTINCT f.sale_id))::numeric(16,3)
  END                                        AS avg_items_per_order
FROM dwh.fact_sale_item f
JOIN dwh.dim_date d
  ON d.date_sk = f.date_sk
JOIN dm.dim_week w
  ON w.iso_year = EXTRACT(ISOYEAR FROM d.full_date)::int
 AND w.iso_week = EXTRACT(WEEK    FROM d.full_date)::int
GROUP BY f.branch_sk, w.week_key
ORDER BY w.week_key, f.branch_sk;

COMMIT;
```

## 7. Порядок запуска

```bash
# 1) Схема DWH (если не создана)
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/20_dwh_schema.sql

# 2) Подключение источников (FDW)
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/21_fdw.sql

# 3) Полная загрузка DWH
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/22_dwh_load.sql

# 4) Витрина: схема и загрузка
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/30_dm_schema.sql
docker compose exec -T db psql -U postgres -d dwh -f /docker/sql/31_dm_load.sql
```

## 8. Проверка корректности

### 8.1. Сверка сумм DM vs DWH
```sql
SELECT
  (SELECT COALESCE(SUM(line_amount),0) FROM dwh.fact_sale_item)   AS dwh_sum,
  (SELECT COALESCE(SUM(revenue_total),0) FROM dm.fact_week_sales) AS dm_sum;
```

### 8.2. Контроль связности и объёма
```sql
-- Наличие недель и строк факта
SELECT COUNT(*) AS weeks_cnt FROM dm.dim_week;
SELECT COUNT(*) AS rows_cnt  FROM dm.fact_week_sales;

-- Отсутствие "осиротевших" ссылок на неделю
SELECT COUNT(*) AS orphans
FROM dm.fact_week_sales f
LEFT JOIN dm.dim_week w ON w.week_key = f.week_key
WHERE w.week_key IS NULL;
```

## 9. Примерные аналитические запросы

```sql
-- ТОП‑10 недель по выручке (все филиалы)
SELECT w.iso_year, w.iso_week, SUM(f.revenue_total) AS revenue
FROM dm.fact_week_sales f
JOIN dm.dim_week w ON w.week_key = f.week_key
GROUP BY w.iso_year, w.iso_week
ORDER BY revenue DESC
LIMIT 10;

-- Динамика выручки по конкретному филиалу (последние 8 недель)
SELECT to_char(w.week_start_date, '\"W\"IW YYYY') AS week_label, SUM(f.revenue_total) AS revenue
FROM dm.fact_week_sales f
JOIN dm.dim_week w ON w.week_key = f.week_key
JOIN dwh.dim_branch b ON b.branch_key = f.branch_key
WHERE b.branch_code = 'EAST'
GROUP BY week_label, w.week_start_date
ORDER BY w.week_start_date DESC
LIMIT 8;

-- Средний чек по филиалам
SELECT b.branch_code,
       ROUND(SUM(f.revenue_total)/NULLIF(SUM(f.orders_count),0), 2) AS avg_check
FROM dm.fact_week_sales f
JOIN dwh.dim_branch b ON b.branch_key = f.branch_key
GROUP BY b.branch_code
ORDER BY avg_check DESC;
```

## 10. Результаты и наблюдения

- Витрина успешно построена: заполнены dm.dim_week и dm.fact_week_sales; суммы по витрине совпадают с суммой по факту DWH.  
- Нулевая агрегация устраняется корректным порядком выполнения: сначала загрузка DWH (22_dwh_load.sql), затем сборка витрины.  
- Средние показатели (avg_check, avg_items_per_order) считаются над уже агрегированными метриками и пригодны для BI‑визуализаций.
