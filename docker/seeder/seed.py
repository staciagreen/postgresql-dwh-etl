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
    "Спортивные товары", "Книги", "Игрушки"
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
