import duckdb

con = duckdb.connect("dev.duckdb")

con.execute(""" 
CREATE TABLE IF NOT EXISTS ga4_event as
SELECT * from
read_csv('../data/raw/ga4/ga4_event_2021.csv',header = True) 
""")

con.close()
print("Data loaded")