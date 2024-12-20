from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
import json

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

# Query to calculate total snowfall per month
query_sum_snow = """
    SELECT station, date, snow 
    FROM snowy_days;
"""

# Fetch all rows from the snowy_days table
rows = session.execute(query_sum_snow)

# Process the data to group by station, year, and month
monthly_snowfall = {}
for row in rows:
    if row.snow is not None:  # Exclude rows with no snowfall data
        station = row.station
        year = row.date.year
        month = row.date.month
        key = (station, year, month)
        monthly_snowfall[key] = monthly_snowfall.get(key, 0) + row.snow

# Insert the aggregated data into the monthly_snowfall table
insert_query = """
    INSERT INTO monthly_snowfall (station, year, month, total_snow)
    VALUES (%s, %s, %s, %s);
"""

for (station, year, month), total_snow in monthly_snowfall.items():
    session.execute(insert_query, (station, year, month, total_snow))

print("Monthly snowfall data successfully aggregated and inserted.")

# Close the connection
cluster.shutdown()
