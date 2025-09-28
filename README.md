# Scalable E-Commerce Data Pipeline & Dashboard

## Project Overview

This project implements a **scalable data engineering pipeline** for a mid-size e-commerce company, handling millions of messy sales records. The goal is to:

1. **Ingest large CSV datasets** efficiently.
2. **Clean and standardize dirty data** (malformed dates, inconsistent regions/categories, invalid numbers).
3. **Transform data** into analytical tables.
4. **Visualize business insights** via an interactive **Streamlit dashboard**.
5. Include **unit tests** to ensure data correctness.

---

## Business Context

- The company sells electronics, fashion, and home goods across India and Southeast Asia.
- Sales team needs insights into:
  - Monthly trends
  - Regional and category performance
  - Discount effectiveness
  - Top-selling products
  - Anomalous transactions

---

## Flow
Generate raw data -> read raw data in chunks -> clean data -> write in partitioned format (month/region) -> create table in duck db on top partitioned parquet files -> create aggregates -> create dashboards

Currently we are using single CSV as source if we need to have incremental data loads then we will use Airflow while maintaining offsets etc.

## Data Schema

| Field Name       | Type   | Description |
|-----------------|--------|------------|
| order_id        | String | Unique order ID (duplicates possible) |
| product_name    | String | Dirty/uncleaned product names |
| category        | String | Product category (variations exist) |
| quantity        | String | Units sold (may be 0/negative/strings) |
| unit_price      | Float  | Price per unit |
| discount_percent| Float  | Discount (0–1, >1 are errors) |
| region          | String | Sales region (e.g., "north", "nort") |
| sale_date       | String | Various formats, some null |
| customer_email  | String | May be null |
| revenue         | Float  | Derived: quantity * unit_price * (1 - discount_percent) |

---

[Watch demo video](demo/Screen%20Recording%202025-09-28%20at%203.02.50%20PM%202.mov)

<img width="1434" height="645" alt="Screenshot 2025-09-28 at 3 14 44 PM" src="https://github.com/user-attachments/assets/0e4a73d4-2798-450f-9d21-cfd557047aef" />

<img width="1397" height="752" alt="Screenshot 2025-09-28 at 3 14 51 PM" src="https://github.com/user-attachments/assets/b28914c1-906d-4a12-8bc3-db5dadea4a76" />

## Project Structure

pipeline-project/
│
├── data/
│ ├── cleaned_parquet/ # Cleaned partitioned Parquet files
│ └── generate_raw_data.py # Script to generate dirty dummy data
│
├── pipeline.py # Main ingestion, cleaning, transformation code
├── analytics.duckdb # DuckDB database storing analytical tables
├── dashboard_app/
│ └── app.py # Streamlit dashboard
├── tests/
│ └── test_pipeline.py # Unit tests for cleaning & transformations
├── requirements.txt
└── README.md



---

## Setup & Installation

1. **Clone the repository**

git clone <repo_url>
cd pipeline-project

1. **Create and activate a virtual environment**

python3 -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows
pip install -r requirements.txt


**Generate Sample Data**
python data/generate_raw_data.py


Generates dirty CSV data in data/raw/

Supports millions of rows for testing scalability

**Run the Pipeline**
python pipeline.py


Pipeline Steps:

Ingest CSV in chunks → handles memory efficiently.

Clean and standardize data:

Convert quantity/unit_price/discount to numeric

Fix invalid discounts (clip 0–1)

Standardize region/category names

Parse sale dates

Compute revenue

Write cleaned data to partitioned Parquet (month → region)

Load into DuckDB and create analytical tables:

monthly_sales_summary

top_products

region_wise_performance

category_discount_map

anomaly_records

**Run the Dashboard**
streamlit run dashboard_app/app.py


Dashboard Features:

Monthly revenue trend (log scale or scaled units)

Top 10 products by revenue

Sales by region

Category discount map

Top 5 anomalous orders

Interactive, readable charts even for billions in revenue

Notes for Large Values:

Y-axis uses log scale or scaled units to handle very large revenue.

Zero or negative revenue values are clipped to 1 for log scale charts.

**Testing**

Unit tests are available in tests/test_pipeline.py.

Run tests:

pytest tests/


Tests cover:

Data cleaning logic

Derived revenue calculation

Aggregation correctness

Edge cases (missing/invalid data)
