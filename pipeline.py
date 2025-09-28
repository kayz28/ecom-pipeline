import os
import pandas as pd
import duckdb
import re

RAW_FILE = "sales_raw.csv"
CLEANED_DIR = "data/cleaned_parquet"
DB_FILE = "analytics.duckdb"

os.makedirs(CLEANED_DIR, exist_ok=True)


# Cleaning function
def clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0.0)
    df = df[pd.to_numeric(df["discount_percent"], errors="coerce").fillna(0.0) <= 1]

    df["region"] = df["region"].astype(str).str.lower().str.strip().apply(
    lambda x: "north" if re.fullmatch(r"n.*(orth)?", x) else
              ("south" if re.fullmatch(r"s.*(outh)?", x) else
              ("east"  if re.fullmatch(r"e.*(ast)?", x) else
              ("west"  if re.fullmatch(r"w.*(est)?", x) else "unknown"))))

    df["category"] = (df["category"]
    .astype(str)
    .str.lower()                          
    .str.strip()                         
    .str.replace(r"[^a-z0-9\s]", "", regex=True) 
    .replace("", "unknown"))

    df["product_name"] = (df["product_name"]
    .astype(str)
    .str.lower()                        
    .str.strip()                       
    .str.replace(r"[^a-z0-9\s]", "", regex=True)
    .replace("", "unknown"))       


    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df["month"] = df["sale_date"].dt.to_period("M").astype(str).fillna("unknown")

    df["customer_email"] = df["customer_email"].fillna("unknown@example.com")

    df["revenue"] = df["quantity"] * df["unit_price"] * (1 - df["discount_percent"])

    return df

# ---------------------------
# Ingestion + Cleaned Parquet writing (partitioned by month -> region)
# ---------------------------
def ingest_and_clean(raw_file=RAW_FILE, cleaned_dir=CLEANED_DIR, chunksize=1_000_000):
    chunk_no = 0
    for chunk in pd.read_csv(raw_file, chunksize=chunksize):
        cleaned = clean_chunk(chunk)

        # Partition by month -> region
        for (month_name, region_name), group in cleaned.groupby(['month', 'region']):
            partition_dir = os.path.join(cleaned_dir, f"month={month_name}", f"region={region_name}")
            print(partition_dir)
            os.makedirs(partition_dir, exist_ok=True)
            file_path = os.path.join(partition_dir, f"chunk_{chunk_no}.parquet")
            group.to_parquet(file_path, index=False)
        
        chunk_no += 1
        print(f"[INFO] Processed chunk {chunk_no}")
    
    print(f"[INFO] Finished cleaning and saving {chunk_no} parquet chunks.")


# ---------------------------
# Transformation: Create analytical tables in DuckDB
# ---------------------------
def create_analytics():
    con = duckdb.connect(DB_FILE)

    # Create view on all cleaned parquet chunks
    con.execute(f"""
        CREATE OR REPLACE VIEW cleaned AS
        SELECT * FROM read_parquet('{CLEANED_DIR}/**/*.parquet')
    """)

    # Monthly sales summary
    con.execute("""
        CREATE OR REPLACE TABLE monthly_sales_summary AS
        SELECT month,
               SUM(revenue) AS total_revenue,
               SUM(quantity) AS total_qty,
               AVG(discount_percent) AS avg_discount
        FROM cleaned
        GROUP BY month
        ORDER BY month
    """)

    # Top 10 products
    con.execute("""
        CREATE OR REPLACE TABLE top_products AS
        SELECT product_name,
               SUM(quantity) AS total_units,
               SUM(revenue) AS total_revenue
        FROM cleaned
        GROUP BY product_name
        ORDER BY total_revenue DESC
        LIMIT 10
    """)

    # Region-wise performance
    con.execute("""
        CREATE OR REPLACE TABLE region_wise_performance AS
        SELECT region,
               SUM(revenue) AS total_revenue,
               SUM(quantity) AS total_qty
        FROM cleaned
        GROUP BY region
        ORDER BY total_revenue DESC
    """)

    # Category discount map
    con.execute("""
        CREATE OR REPLACE TABLE category_discount_map AS
        SELECT category,
               AVG(discount_percent) AS avg_discount
        FROM cleaned
        GROUP BY category
        ORDER BY avg_discount DESC
    """)

    # Top 5 anomaly orders
    con.execute("""
        CREATE OR REPLACE TABLE anomaly_records AS
        SELECT order_id,
               SUM(revenue) AS order_revenue,
               COUNT(*) AS line_items
        FROM cleaned
        GROUP BY order_id
        ORDER BY order_revenue DESC
        LIMIT 5
    """)

    #revenue each category made per month
    con.execute("""
    CREATE TABLE category_sales_trend AS
    SELECT 
        month,
        category,
        SUM(revenue) AS total_revenue,
        SUM(quantity) AS total_units
    FROM cleaned
    GROUP BY month, category
    ORDER BY month, category;
""")
    #For each customer, total orders, revenue, and average discount  
    con.execute("""
        CREATE TABLE customer_order_summary AS
        SELECT
            customer_email,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(revenue) AS total_revenue,
            AVG(discount_percent) AS avg_discount
        FROM cleaned
        WHERE customer_email IS NOT NULL
        GROUP BY customer_email
        ORDER BY total_revenue DESC;
""")
    #discounts affect revenue per category
    con.execute("""
        CREATE TABLE discount_effectiveness AS
        SELECT
            category,
            AVG(discount_percent) AS avg_discount,
            SUM(revenue) AS total_revenue
        FROM cleaned
        GROUP BY category
        ORDER BY total_revenue DESC;
""")
    
    print(con.execute("SHOW TABLES").fetchall())
    df = con.execute(f"SELECT * FROM read_parquet('{CLEANED_DIR}/**/*.parquet') LIMIT 5").df()
    print(df)
    df = con.execute("""
    SELECT *
    FROM monthly_sales_summary
""").fetchdf()
    print(df)
    print("[INFO] Analytical tables created in DuckDB.")


if __name__ == "__main__":
    ingest_and_clean(RAW_FILE, CLEANED_DIR, 1_000_000)
    create_analytics()
