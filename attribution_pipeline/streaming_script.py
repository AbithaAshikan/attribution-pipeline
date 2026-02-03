import duckdb, time, random
from datetime import datetime, timezone
import streamlit as st
import pandas as pd

# single connection
con = duckdb.connect(":memory:")

# create tables
con.execute("""
CREATE TABLE ga4_event (
    event_date TEXT, event_timestamp BIGINT, event_name TEXT,
    user_pseudo_id BIGINT, "traffic_source.source" TEXT,
    "traffic_source.medium" TEXT, "traffic_source.name" TEXT
)
""")
con.execute("""
CREATE TABLE final_attribution (
    user_pseudo_id BIGINT PRIMARY KEY,
    first_click_source TEXT, first_click_time BIGINT,
    last_click_source TEXT, last_click_time BIGINT
)
""")

# Streamlit setup
st.set_page_config(page_title="GA4 Attribution Dashboard", layout="wide")
st.title("GA4 Attribution Dashboard")

placeholder1 = st.empty()
placeholder2 = st.empty()
recent_events_placeholder = st.empty()

# streaming + dashboard in one loop
while True:
    # Insert new event
    event_ts = int(time.time() * 1_000_000)
    uid = random.randint(1000000, 9999999)
    src = random.choice(["<Other>", "google"])
    con.execute("""
        INSERT INTO ga4_event (event_date, event_timestamp, event_name, user_pseudo_id,
                               "traffic_source.source", "traffic_source.medium", "traffic_source.name")
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [datetime.now(timezone.utc).strftime("%Y%m%d"), event_ts, random.choice(["page_view","purchase"]),
          uid, src, random.choice(["cpc","organic","referral"]), random.choice(["(organic)","(referral)"])])
    con.commit()

    # Compute attribution
    con.execute("""
        INSERT OR REPLACE INTO final_attribution
        SELECT user_pseudo_id,
               FIRST_VALUE("traffic_source.source") OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ASC) AS first_click_source,
               MIN(event_timestamp) OVER (PARTITION BY user_pseudo_id) AS first_click_time,
               FIRST_VALUE("traffic_source.source") OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp DESC) AS last_click_source,
               MAX(event_timestamp) OVER (PARTITION BY user_pseudo_id) AS last_click_time
        FROM ga4_event
    """)
    con.commit()

    # Update charts
    first_click_df = con.execute("SELECT first_click_source AS source, COUNT(*) AS count FROM final_attribution GROUP BY 1").df()
    last_click_df = con.execute("SELECT last_click_source AS source, COUNT(*) AS count FROM final_attribution GROUP BY 1").df()
    recent_df = con.execute("SELECT user_pseudo_id, event_name, \"traffic_source.source\", event_timestamp FROM ga4_event ORDER BY event_timestamp DESC LIMIT 10").df()

    placeholder1.subheader("First-Click Attribution")
    placeholder1.bar_chart(first_click_df.set_index("source") if not first_click_df.empty else pd.DataFrame())

    placeholder2.subheader("Last-Click Attribution")
    placeholder2.bar_chart(last_click_df.set_index("source") if not last_click_df.empty else pd.DataFrame())

    recent_events_placeholder.subheader("Recent Events")
    recent_events_placeholder.table(recent_df)

    time.sleep(3)  # refresh interval
