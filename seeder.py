import psycopg2
from faker import Faker
import random

# Подключение к базе данных PostgreSQL
conn = psycopg2.connect(
    dbname="your_db_name", user="your_user", password="your_password", host="localhost", port="5432"
)
cursor = conn.cursor()

# Инициализация Faker
fake = Faker()

# Генерация и вставка данных для таблиц
def generate_customers(n):
    for _ in range(n):
        name = fake.name()
        cursor.execute("INSERT INTO customer (customer_name) VALUES (%s)", (name,))
    print(f"{n} customers added")

def generate_categories(n):
    categories = ['Электроника', 'Одежда', 'Продукты питания', 'Мебель', 'Игрушки', 'Косметика', 'Книги', 'Техника', 'Обувь', 'Детские товары']
    for category in categories:
        cursor.execute("INSERT INTO category (category_name) VALUES (%s)", (category,))
    print(f"{len(categories)} categories added")

def generate_products(n):
    for _ in range(n):
        name = fake.word()
        price = round(random.uniform(500, 50000), 2)
        cursor.execute("INSERT INTO product (product_name, list_price) VALUES (%s, %s)", (name, price))
    print(f"{n} products added")

def generate_sales(n):
    for _ in range(n):
        customer_id = random.randint(1, n)
        sale_date = fake.date_this_year()
        total_amount = round(random.uniform(1000, 50000), 2)
        cursor.execute("INSERT INTO sale (customer_id, sale_date, total_amount) VALUES (%s, %s, %s)",
                       (customer_id, sale_date, total_amount))
    print(f"{n} sales added")

def generate_sale_items(n):
    for _ in range(n):
        sale_id = random.randint(1, n)
        product_id = random.randint(1, n)
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(1000, 50000), 2)
        line_amount = quantity * unit_price
        cursor.execute("INSERT INTO sale_item (sale_id, product_id, quantity, unit_price, line_amount) VALUES (%s, %s, %s, %s, %s)",
                       (sale_id, product_id, quantity, unit_price, line_amount))
    print(f"{n} sale items added")

# Сидирование
def seed_database():
    generate_customers(25)
    generate_categories(10)
    generate_products(25)
    generate_sales(25)
    generate_sale_items(50)

    # Применить изменения
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    seed_database()
