import streamlit as st
import duckdb
import pandas as pd
import time

st.set_page_config(page_title="GA4 Attribution Dashboard", layout="wide")
st.title("GA4 Attribution Dashboard (Near Real-Time)")

con = duckdb.connect("dev.duckdb")

refresh_interval = 10

while True:
    first_click_df = con.execute("""
        SELECT first_click_source, COUNT(*) AS count
        FROM final_attribution
        GROUP BY 1
    """).df()

    # Query first-click attribution
    last_click_df = con.execute("""
        SELECT last_click_source, COUNT(*) AS count
        FROM final_attribution
        GROUP BY 1
    """).df()

    # Layout: two columns
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("First-Click Attribution")
        st.bar_chart(first_click_df.rename(columns={'first_click_source': 'Source'}).set_index('Source'))

    with col2:
        st.subheader("Last-Click Attribution")
        st.bar_chart(last_click_df.rename(columns={'last_click_source': 'Source'}).set_index('Source'))

    # Sleep before next refresh
    time.sleep(refresh_interval)
