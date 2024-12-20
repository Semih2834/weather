import requests
import pandas as pd
import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from datetime import datetime
from math import isnan 

# Connect to Cassandra
cloud_config= {
  'secure_connect_bundle': 'secure-connect-linkedin.zip'
}
with open("linkedin-token.json") as f:
    secrets = json.load(f)
CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]
auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

# Switch to the project keyspace
session.set_keyspace('project')

# Create the rainy_days table
session.execute("""
CREATE TABLE IF NOT EXISTS rainy_days (
    station text,
    date date,
    humidity float,
    prcp float,
    snow float,
    tmax float,
    tmin float,
    wind float,
    PRIMARY KEY (station, date)
)
""")
print("Table rainy_days created.")

# Query to fetch entries with snow > 0
select_query = "SELECT * FROM new_jersey_weather WHERE prcp > 0 ALLOW FILTERING"

# Prepare insert query for the new table
insert_query = """
INSERT INTO rainy_days (station, date, humidity, prcp, snow, tmax, tmin, wind)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Execute select query
rows = session.execute(select_query)

# Insert into the new table
for row in rows:
    try:
        # Check if prcp is NaN and skip such rows
        if row.snow is None or isnan(row.snow):
            print(f"Skipping row due to NaN prcp value: {row}")
            continue

        # Debugging: Print the row data
        print(f"Inserting row: {row}")
        
        # Extract and sanitize values
        station = row.station
        date = row.date
        humidity = row.humidity if row.humidity is not None else 0.0
        prcp = row.prcp if row.prcp is not None else 0.0
        snow = row.snow
        tmax = row.tmax if row.tmax is not None else 0.0
        tmin = row.tmin if row.tmin is not None else 0.0
        wind = row.wind if row.wind is not None else 0.0

        # Execute insert query
        session.execute(insert_query, (station, date, humidity, prcp, snow, tmax, tmin, wind))
    except Exception as e:
        print(f"Error inserting row {row}: {e}")

print("Filtered data inserted into rainy_days table.")

# Close connection
cluster.shutdown()

