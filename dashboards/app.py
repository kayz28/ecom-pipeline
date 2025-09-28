import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

DB_FILE = "analytics.duckdb"
con = duckdb.connect(DB_FILE)

st.set_page_config(page_title="E-Commerce Sales Dashboard", layout="wide")
st.title("E-Commerce Sales Dashboard")

# Monthly Revenue Trend
monthly = con.execute("SELECT * FROM monthly_sales_summary").df()

# Option 1: Use log scale for extreme variation
fig1 = px.line(
    monthly,
    x="month",
    y="total_revenue",
    title="Monthly Revenue Trend",
    markers=True
)
fig1.update_yaxes(type="log", title="Revenue (Log Scale)", tickformat=",")
fig1.update_xaxes(title="Month")
st.plotly_chart(fig1, use_container_width=True)

# Top 10 Products
top_products = con.execute("SELECT * FROM top_products").df()
fig2 = px.bar(top_products, x="product_name", y="total_revenue", title="Top 10 Products")
fig2.update_yaxes(type="log", title="Total Revenue (Log Scale)", tickformat=",")
fig2.update_xaxes(title="product_name")
st.plotly_chart(fig2, use_container_width=True)

# Sales by Region
region = con.execute("SELECT * FROM region_wise_performance").df()
fig3 = px.bar(region, x="region", y="total_revenue", title="Sales by Region")
fig3.update_yaxes(type="log", title="Total Revenue (Log Scale)", tickformat=",")
fig3.update_xaxes(title="region")
st.plotly_chart(fig3, use_container_width=True)

# Category Discount Heatmap
category = con.execute("SELECT * FROM category_discount_map").df()
fig4 = px.bar(category, x="category", y="avg_discount", title="Category Discount Map")
fig4.update_yaxes(type="log", title="Avg Discount", tickformat=",")
fig4.update_xaxes(title="category")
st.plotly_chart(fig4, use_container_width=True)

# Anomaly Records
anomalies = con.execute("SELECT * FROM anomaly_records").df()
st.subheader("Top 5 Anomalous Orders")
st.dataframe(anomalies)

#category sales trend
category_trend = con.execute("SELECT * FROM category_sales_trend").df()
fig6 = px.line(category_trend, x="month", y="total_revenue", color="category",
               title="Category Revenue Trend")
fig6.update_yaxes(type="log", title="total_revenue", tickformat=",")
fig6.update_xaxes(title="month")
st.plotly_chart(fig6, use_container_width=True)


# Customer Order Summary (Top 20 customers)
customers = con.execute("SELECT * FROM customer_order_summary ORDER BY total_revenue DESC LIMIT 20").df()
fig7 = px.bar(customers, x="customer_email", y="total_revenue", title="Top 20 Customers")
st.plotly_chart(fig7, use_container_width=True)

# ------------------------
# 8. Discount Effectiveness (Log Scale)
# ------------------------
discount_eff = con.execute("SELECT * FROM discount_effectiveness").df()
fig8 = px.scatter(discount_eff, x="avg_discount", y="total_revenue", 
                  color="category", size="total_revenue",
                  title="Discount Effectiveness by Category (Log Scale)")  # log scale for revenue
fig8.update_yaxes(type="log", title="total_revenue", tickformat=",")
fig8.update_xaxes(title="avg_discount")
st.plotly_chart(fig8, use_container_width=True)
