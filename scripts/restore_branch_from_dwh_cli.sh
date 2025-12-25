#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <branch_code e.g. west> <start_date> <end_date>"
  exit 2
fi

BRANCH="$1"
START_DATE="$2"
END_DATE="$3"
LOG=/tmp/restore_branch_${BRANCH}_${START_DATE}_${END_DATE}.log
: > "$LOG"

echo "Restore branch $BRANCH from $START_DATE to $END_DATE" | tee -a "$LOG"

PSQL_DWH="docker compose exec -T db psql -U postgres -d dwh -t -A -F '|'"
PSQL_BRANCH="docker compose exec -T db psql -U postgres -d branch_${BRANCH} -t -A -F '|'"

TMP_SALE_MAP=$(mktemp)
: > "$TMP_SALE_MAP"

# helper to escape single quotes for SQL literals
escape_sql() {
  printf "%s" "$1" | sed "s/'/''/g"
}

# 1) Customers
echo "\n-- Restoring customers" | tee -a "$LOG"
$PSQL_DWH -c "SELECT DISTINCT dc.customer_id, dc.customer_name FROM dwh.dim_customer dc JOIN dwh.fact_sale_item f ON f.customer_key=dc.customer_key JOIN dwh.dim_date dd ON f.date_key=dd.date_key WHERE dc.branch_key = (SELECT branch_key FROM dwh.dim_branch WHERE branch_code = '$BRANCH') AND dd.full_date BETWEEN '$START_DATE' AND '$END_DATE' ORDER BY dc.customer_id;" | while IFS='|' read -r orig_id name; do
  esc_name=$(escape_sql "$name")
  exists=$($PSQL_BRANCH -c "SELECT customer_id FROM customer WHERE customer_name = '$esc_name' LIMIT 1;" | tr -d '[:space:]' ) || true
  if [ -z "$exists" ]; then
    new_id=$($PSQL_BRANCH -c "INSERT INTO customer (customer_name, rowguid, ModifiedDate) VALUES ('$esc_name', gen_random_uuid(), NOW()) RETURNING customer_id;" | tr -d '[:space:]')
    echo "Customer: orig=$orig_id name=$name -> new=$new_id" | tee -a "$LOG"
  else
    echo "Customer already exists: orig=$orig_id name=$name -> existing=$exists" | tee -a "$LOG"
  fi
done

# 2) Categories
echo "\n-- Restoring categories" | tee -a "$LOG"
$PSQL_DWH -c "SELECT DISTINCT dcat.category_id, dcat.category_name FROM dwh.dim_category dcat JOIN dwh.bridge_product_category bc ON bc.category_key = dcat.category_key JOIN dwh.dim_product dp ON dp.product_key = bc.product_key WHERE dcat.branch_key = (SELECT branch_key FROM dwh.dim_branch WHERE branch_code = '$BRANCH') AND dp.branch_key = dcat.branch_key;" | while IFS='|' read -r orig_id name; do
  esc_name=$(escape_sql "$name")
  exists=$($PSQL_BRANCH -c "SELECT category_id FROM category WHERE category_name = '$esc_name' LIMIT 1;" | tr -d '[:space:]') || true
  if [ -z "$exists" ]; then
    new_id=$($PSQL_BRANCH -c "INSERT INTO category (category_name, rowguid, ModifiedDate) VALUES ('$esc_name', gen_random_uuid(), NOW()) RETURNING category_id;" | tr -d '[:space:]')
    echo "Category: orig=$orig_id name=$name -> new=$new_id" | tee -a "$LOG"
  else
    echo "Category already exists: orig=$orig_id name=$name -> existing=$exists" | tee -a "$LOG"
  fi
done

# 3) Products
echo "\n-- Restoring products" | tee -a "$LOG"
$PSQL_DWH -c "SELECT DISTINCT dp.product_id, dp.product_name, dp.list_price FROM dwh.dim_product dp JOIN dwh.fact_sale_item f ON f.product_key=dp.product_key JOIN dwh.dim_date dd ON f.date_key=dd.date_key WHERE dp.branch_key = (SELECT branch_key FROM dwh.dim_branch WHERE branch_code = '$BRANCH') AND dd.full_date BETWEEN '$START_DATE' AND '$END_DATE' ORDER BY dp.product_id;" | while IFS='|' read -r orig_id name price; do
  esc_name=$(escape_sql "$name")
  exists=$($PSQL_BRANCH -c "SELECT product_id FROM product WHERE product_name = '$esc_name' AND list_price = $price LIMIT 1;" | tr -d '[:space:]') || true
  if [ -z "$exists" ]; then
    new_id=$($PSQL_BRANCH -c "INSERT INTO product (product_name, list_price, rowguid, ModifiedDate) VALUES ('$esc_name', $price, gen_random_uuid(), NOW()) RETURNING product_id;" | tr -d '[:space:]')
    echo "Product: orig=$orig_id name=$name price=$price -> new=$new_id" | tee -a "$LOG"
  else
    echo "Product exists: orig=$orig_id -> existing=$exists" | tee -a "$LOG"
  fi
done

# 4) Product categories (bridge)
echo "\n-- Restoring product_category relations" | tee -a "$LOG"
$PSQL_DWH -c "SELECT dp.product_id, dcat.category_id FROM dwh.bridge_product_category bc JOIN dwh.dim_product dp ON dp.product_key=bc.product_key JOIN dwh.dim_category dcat ON dcat.category_key=bc.category_key WHERE dp.branch_key = (SELECT branch_key FROM dwh.dim_branch WHERE branch_code = '$BRANCH') AND dcat.branch_key = dp.branch_key;" | while IFS='|' read -r prod_orig cat_orig; do
  # get names from DWH
  prod_name=$($PSQL_DWH -c "SELECT product_name FROM dwh.dim_product WHERE product_id = $prod_orig LIMIT 1;" | tr -d '[:space:]') || true
  cat_name=$($PSQL_DWH -c "SELECT category_name FROM dwh.dim_category WHERE category_id = $cat_orig LIMIT 1;" | tr -d '[:space:]') || true
  if [ -z "$prod_name" ] || [ -z "$cat_name" ]; then
    echo "Skipping bridge mapping orig_prod=$prod_orig cat=$cat_orig: missing names in DWH" | tee -a "$LOG"
    continue
  fi
  esc_prod=$(escape_sql "$prod_name")
  esc_cat=$(escape_sql "$cat_name")
  new_prod=$($PSQL_BRANCH -c "SELECT product_id FROM product WHERE product_name = '$esc_prod' LIMIT 1;" | tr -d '[:space:]') || true
  new_cat=$($PSQL_BRANCH -c "SELECT category_id FROM category WHERE category_name = '$esc_cat' LIMIT 1;" | tr -d '[:space:]') || true
  if [ -n "$new_prod" ] && [ -n "$new_cat" ]; then
    $PSQL_BRANCH -c "INSERT INTO product_category (product_id, category_id, rowguid, ModifiedDate) VALUES ($new_prod, $new_cat, gen_random_uuid(), NOW()) ON CONFLICT DO NOTHING;" | tee -a "$LOG"
    echo "Bridge mapped: prod_orig=$prod_orig cat_orig=$cat_orig -> $new_prod/$new_cat" | tee -a "$LOG"
  else
    echo "Skipping bridge: missing mapping in branch for prod_orig=$prod_orig or cat_orig=$cat_orig" | tee -a "$LOG"
  fi
done

# 5) Sales (headers)
echo "\n-- Restoring sales" | tee -a "$LOG"
$PSQL_DWH -c "SELECT f.sale_id, dd.full_date, dc.customer_id, SUM(f.line_amount) FROM dwh.fact_sale_item f JOIN dwh.dim_date dd ON f.date_key=dd.date_key JOIN dwh.dim_customer dc ON f.customer_key=dc.customer_key WHERE f.branch_key = (SELECT branch_key FROM dwh.dim_branch WHERE branch_code = '$BRANCH') AND dd.full_date BETWEEN '$START_DATE' AND '$END_DATE' GROUP BY f.sale_id, dd.full_date, dc.customer_id ORDER BY dd.full_date, f.sale_id;" | while IFS='|' read -r orig_sale sale_date orig_cust total; do
  # find mapped customer id in branch
  cust_name=$($PSQL_DWH -c "SELECT customer_name FROM dwh.dim_customer WHERE customer_id = $orig_cust LIMIT 1;" | tr -d '[:space:]') || true
  esc_cust=$(escape_sql "$cust_name")
  new_cust=$($PSQL_BRANCH -c "SELECT customer_id FROM customer WHERE customer_name = '$esc_cust' LIMIT 1;" | tr -d '[:space:]') || true
  if [ -z "$new_cust" ]; then
    echo "ERROR: no mapped customer for orig_cust=$orig_cust name=$cust_name" | tee -a "$LOG"
    continue
  fi
  new_sale=$($PSQL_BRANCH -c "INSERT INTO sale (customer_id, sale_date, total_amount, rowguid, ModifiedDate) VALUES ($new_cust, '$sale_date', $total, gen_random_uuid(), NOW()) RETURNING sale_id;" | tr -d '[:space:]')
  echo "Sale: orig=$orig_sale date=$sale_date orig_cust=$orig_cust -> new_sale=$new_sale" | tee -a "$LOG"
  # store mapping in a temp file for sale items mapping
  echo "$orig_sale|$new_sale" >> "$TMP_SALE_MAP"
done

# 6) Sale items
echo "\n-- Restoring sale_items" | tee -a "$LOG"
$PSQL_DWH -c "SELECT f.sale_item_id, f.sale_id, f.product_key, f.quantity, f.unit_price, f.line_amount FROM dwh.fact_sale_item f JOIN dwh.dim_date dd ON f.date_key=dd.date_key WHERE f.branch_key = (SELECT branch_key FROM dwh.dim_branch WHERE branch_code = '$BRANCH') AND dd.full_date BETWEEN '$START_DATE' AND '$END_DATE' ORDER BY f.sale_id, f.sale_item_id;" | while IFS='|' read -r orig_item orig_sale prod_key qty unit_price line_amount; do
  # map product_key -> product_id (orig product id in dim_product)
  prod_orig=$($PSQL_DWH -c "SELECT product_id FROM dwh.dim_product WHERE product_key = $prod_key LIMIT 1;" | tr -d '[:space:]') || true
  prod_name=$($PSQL_DWH -c "SELECT product_name FROM dwh.dim_product WHERE product_id = $prod_orig LIMIT 1;" | tr -d '[:space:]') || true
  esc_prod=$(escape_sql "$prod_name")
  new_prod=$($PSQL_BRANCH -c "SELECT product_id FROM product WHERE product_name = '$esc_prod' LIMIT 1;" | tr -d '[:space:]') || true
  # find new sale id from mapping
  new_sale=$(grep "^$orig_sale|" "$TMP_SALE_MAP" | cut -d'|' -f2 | tr -d '[:space:]' || true)
  if [ -z "$new_sale" ] || [ -z "$new_prod" ]; then
    echo "Skipping sale_item orig=$orig_item: missing mapping sale=$orig_sale -> $new_sale prod_key=$prod_key -> $new_prod" | tee -a "$LOG"
    continue
  fi
  $PSQL_BRANCH -c "INSERT INTO sale_item (sale_id, product_id, quantity, unit_price, line_amount, rowguid, ModifiedDate) VALUES ($new_sale, $new_prod, $qty, $unit_price, $line_amount, gen_random_uuid(), NOW());" | tee -a "$LOG"
  echo "Inserted sale_item for new_sale=$new_sale prod=$new_prod qty=$qty" | tee -a "$LOG"
done

# Summary
echo "\nRestore finished. Log: $LOG" | tee -a "$LOG"

# cleanup
rm -f "$TMP_SALE_MAP"

exit 0

