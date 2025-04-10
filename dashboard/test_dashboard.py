import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Get Redshift credentials
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

# Connect to Redshift
conn = psycopg2.connect(
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    dbname=REDSHIFT_DB,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD
)

query = """
SELECT *
FROM dbt_sp.fact_trips
LIMIT 1000;
"""

df = pd.read_sql(query, conn)
conn.close()

# Streamlit Dashboard
st.title("🚦 Dublin TRIPS Travel Time Dashboard")

# Filters
routes = df["route"].unique()
selected_routes = st.multiselect("Select Routes", routes, default=routes[:3])

filtered_df = df[df["route"].isin(selected_routes)]

# Charts
st.subheader("📈 Average Travel Time Over Time")
st.line_chart(filtered_df.groupby("date")["avg_travel_time"].mean())

st.subheader("📊 Trip Count by Route")
st.bar_chart(filtered_df.groupby("route")["trip_count"].sum())

st.subheader("🧾 Raw Data Preview")
st.dataframe(filtered_df.head(20))
