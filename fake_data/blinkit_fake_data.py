import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# Connect to your local Postgres DB
conn = psycopg2.connect(
    dbname="blinkit_db", user="postgres", password="12345678", host="localhost", port="5432"
)
cur = conn.cursor()

# Drop existing tables if needed
cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")

# Create schema
cur.execute("""
-- 1. Category
CREATE TABLE category (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE
);

-- 2. Brand
CREATE TABLE brand (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE
);

-- 3. Unit
CREATE TABLE unit (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    abbreviation VARCHAR(10)
);

-- 4. Product
CREATE TABLE product (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    category_id INTEGER REFERENCES category(id),
    brand_id INTEGER REFERENCES brand(id),
    unit_id INTEGER REFERENCES unit(id)
);

-- 5. Price
CREATE TABLE price (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES product(id),
    price NUMERIC(10,2),
    effective_from TIMESTAMP
);

-- 6. Discount
CREATE TABLE discount (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES product(id),
    discount_percent NUMERIC(5,2),
    start_date DATE,
    end_date DATE
);

-- 7. Inventory
CREATE TABLE inventory (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES product(id),
    quantity INTEGER,
    updated_at TIMESTAMP
);

-- 8. City
CREATE TABLE city (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

-- 9. Warehouse
CREATE TABLE warehouse (
    id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES city(id),
    address TEXT
);

-- 10. User
CREATE TABLE app_user (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100) UNIQUE,
    city_id INTEGER REFERENCES city(id)
);

-- 11. Address
CREATE TABLE user_address (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES app_user(id),
    address TEXT
);

-- 12. Order
CREATE TABLE app_order (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES app_user(id),
    warehouse_id INTEGER REFERENCES warehouse(id),
    order_time TIMESTAMP,
    status VARCHAR(50)
);

-- 13. OrderItem
CREATE TABLE order_item (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES app_order(id),
    product_id INTEGER REFERENCES product(id),
    quantity INTEGER,
    price_at_purchase NUMERIC(10,2)
);

-- 14. DeliverySlot
CREATE TABLE delivery_slot (
    id SERIAL PRIMARY KEY,
    start_time TIME,
    end_time TIME
);

-- 15. Delivery
CREATE TABLE delivery (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES app_order(id),
    delivery_slot_id INTEGER REFERENCES delivery_slot(id),
    delivered_at TIMESTAMP
);
""")
conn.commit()

# Insert fake data
NUM_PRODUCTS = 50
NUM_USERS = 20

# Category
categories = [fake.word().capitalize() for _ in range(10)]
cur.executemany("INSERT INTO category (name) VALUES (%s)", [(c,) for c in categories])

# Brand
brands = [fake.company() for _ in range(10)]
cur.executemany("INSERT INTO brand (name) VALUES (%s)", [(b,) for b in brands])

# Unit
units = [("Kilogram", "kg"), ("Gram", "g"), ("Litre", "L"), ("Piece", "pc"), ("Pack", "pk")]
cur.executemany("INSERT INTO unit (name, abbreviation) VALUES (%s, %s)", units)

# City
cities = [fake.city() for _ in range(5)]
cur.executemany("INSERT INTO city (name) VALUES (%s)", [(c,) for c in cities])

# Commit base tables
conn.commit()

# Get foreign keys
cur.execute("SELECT id FROM category"); category_ids = [r[0] for r in cur.fetchall()]
cur.execute("SELECT id FROM brand"); brand_ids = [r[0] for r in cur.fetchall()]
cur.execute("SELECT id FROM unit"); unit_ids = [r[0] for r in cur.fetchall()]
cur.execute("SELECT id FROM city"); city_ids = [r[0] for r in cur.fetchall()]

# Product
products = []
for _ in range(NUM_PRODUCTS):
    products.append((
        fake.word().capitalize() + " " + fake.word().capitalize(),
        random.choice(category_ids),
        random.choice(brand_ids),
        random.choice(unit_ids)
    ))
cur.executemany("INSERT INTO product (name, category_id, brand_id, unit_id) VALUES (%s, %s, %s, %s)", products)
conn.commit()

# Get product IDs
cur.execute("SELECT id FROM product"); product_ids = [r[0] for r in cur.fetchall()]

# Price
prices = []
for pid in product_ids:
    prices.append((pid, round(random.uniform(10, 500), 2), fake.date_time_between(start_date='-30d', end_date='now')))
cur.executemany("INSERT INTO price (product_id, price, effective_from) VALUES (%s, %s, %s)", prices)

# Discount
discounts = []
for pid in random.sample(product_ids, k=int(NUM_PRODUCTS/2)):
    start = datetime.now() - timedelta(days=random.randint(1, 15))
    end = start + timedelta(days=random.randint(1, 10))
    discounts.append((pid, round(random.uniform(5, 40), 2), start.date(), end.date()))
cur.executemany("INSERT INTO discount (product_id, discount_percent, start_date, end_date) VALUES (%s, %s, %s, %s)", discounts)

# Inventory
inventory = [(pid, random.randint(0, 100), datetime.now()) for pid in product_ids]
cur.executemany("INSERT INTO inventory (product_id, quantity, updated_at) VALUES (%s, %s, %s)", inventory)

# Warehouse
warehouses = [(random.choice(city_ids), fake.address()) for _ in range(5)]
cur.executemany("INSERT INTO warehouse (city_id, address) VALUES (%s, %s)", warehouses)

# Users
users = [(fake.name(), fake.email(), random.choice(city_ids)) for _ in range(NUM_USERS)]
cur.executemany("INSERT INTO app_user (name, email, city_id) VALUES (%s, %s, %s)", users)

# Addresses
cur.execute("SELECT id FROM app_user"); user_ids = [r[0] for r in cur.fetchall()]
addresses = [(uid, fake.address()) for uid in user_ids]
cur.executemany("INSERT INTO user_address (user_id, address) VALUES (%s, %s)", addresses)

# Orders
cur.execute("SELECT id FROM warehouse"); warehouse_ids = [r[0] for r in cur.fetchall()]
orders = []
statuses = ["placed", "packed", "dispatched", "delivered"]
for uid in user_ids:
    for _ in range(random.randint(1, 3)):
        orders.append((
            uid,
            random.choice(warehouse_ids),
            fake.date_time_between(start_date='-10d', end_date='now'),
            random.choice(statuses)
        ))
cur.executemany("INSERT INTO app_order (user_id, warehouse_id, order_time, status) VALUES (%s, %s, %s, %s)", orders)

# Order Items
cur.execute("SELECT id FROM app_order"); order_ids = [r[0] for r in cur.fetchall()]
order_items = []
for oid in order_ids:
    for _ in range(random.randint(1, 5)):
        pid = random.choice(product_ids)
        order_items.append((oid, pid, random.randint(1, 5), round(random.uniform(10, 500), 2)))
cur.executemany("INSERT INTO order_item (order_id, product_id, quantity, price_at_purchase) VALUES (%s, %s, %s, %s)", order_items)

# Delivery Slots
slots = [(datetime.strptime(f"{h:02}:00", "%H:%M").time(), datetime.strptime(f"{h+1:02}:00", "%H:%M").time()) for h in range(8, 20)]
cur.executemany("INSERT INTO delivery_slot (start_time, end_time) VALUES (%s, %s)", slots)

# Deliveries
cur.execute("SELECT id FROM delivery_slot"); slot_ids = [r[0] for r in cur.fetchall()]
deliveries = []
for oid in random.sample(order_ids, k=int(len(order_ids)*0.7)):
    deliveries.append((oid, random.choice(slot_ids), datetime.now() - timedelta(hours=random.randint(1, 48))))
cur.executemany("INSERT INTO delivery (order_id, delivery_slot_id, delivered_at) VALUES (%s, %s, %s)", deliveries)

conn.commit()
cur.close()
conn.close()

print("✅ Blinkit database with 15+ tables and fake data created successfully.")
