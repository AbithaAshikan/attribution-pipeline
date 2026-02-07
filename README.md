# Near-Real-Time Attribution Pipeline (DuckDB + dbt, GA4-style)
This project demonstrates how to build a modern analytics engineering pipeline that computes First-Click and Last-Click attribution on GA4 event data using DuckDB and dbt, with a near-real-time streaming demo.

# Architecture Overview
1. Initialize DuckDB and dbt project
2. Model GA4 raw event data
3. Ingest batch and streaming events
4. Clean and normalize data using dbt staging models
5. Define session and conversion logic
6. Compute First-Click and Last-Click attribution
7. Refresh models in near-real-time
8. Expose results for analytics and dashboards

# Tech Stack

DuckDB – analytical database
dbt – data transformations and modeling
Python – streaming event producer and data ingestion
SQL – attribution logic
Streamlit - dashborad

# Attribution Logic

Attribution is calculated at the user_id level.

# Near-Real-Time Streaming Demo

- Events are generated using Python
- Events are appended to DuckDB
- Attribution results update with low latency

# How to run
dbt run - to run entire dbt model
dbt run -select <model_name> - to run particular model
streamlit run 

