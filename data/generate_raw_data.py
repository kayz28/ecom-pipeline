import csv
import random
from faker import Faker
from tqdm import tqdm

fake = Faker()

NUM_ROWS = 100_000_000  # 100 million
CHUNK_SIZE = 1_000_000  # write 1 million rows at a time

PRODUCT_NAMES = ["Laptop", "Phone", "Headphones", "Camera", "Shoes", "Watch"]
CATEGORIES = ["electronics", "fashion", "home appliance"]
REGIONS = ["North", "South", "East", "West"]

file_path = "sales_raw.csv"

# Write header first
with open(file_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "order_id","product_name","category","quantity","unit_price",
        "discount_percent","region","sale_date","customer_email"
    ])

# Generate data in chunks
rows_written = 0
for chunk_start in tqdm(range(0, NUM_ROWS, CHUNK_SIZE), desc="Generating 100M rows"):
    chunk = []
    for _ in range(min(CHUNK_SIZE, NUM_ROWS - rows_written)):
        order_id = f"ORD{random.randint(1, NUM_ROWS//2)}"  # duplicates
        product_name = random.choice(PRODUCT_NAMES)
        category = random.choice(CATEGORIES)
        quantity = random.choice([random.randint(0,10), str(random.randint(-5,15)), ""])
        unit_price = random.choice([round(random.uniform(10,1000),2), ""])
        discount_percent = random.choice([round(random.uniform(0,1),2), round(random.uniform(1.1,2.0),2), ""])
        region = random.choice(REGIONS)
        sale_date = random.choice([fake.date_between(start_date='-2y', end_date='today').isoformat(), ""])
        customer_email = random.choice([fake.email(), None])
        chunk.append([order_id, product_name, category, quantity, unit_price,
                      discount_percent, region, sale_date, customer_email])
    # write this chunk to file
    with open(file_path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(chunk)
    rows_written += len(chunk)

