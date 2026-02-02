import duckdb
import time
import random
from datetime import datetime

con = duckdb.connect("dev.duckdb")

while True:
    con.execute("""
        INSERT INTO ga4_event VALUES (
            ?, ?, ?, ?
        )
    """, (
        f"user_{random.randint(1,100)}",
        random.choice(["google", "facebook", "email"]),
        int(time.time() * 1_000_000),
        datetime.utcnow().strftime("%Y%m%d")
    ))

    time.sleep(5)
