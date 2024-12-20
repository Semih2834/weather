from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import matplotlib.pyplot as plt
from collections import Counter
import json
from datetime import datetime, timedelta

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
session = cluster.connect('project')

# Query to fetch date data from snowy_days table
query = "SELECT date FROM snowy_days"
rows = session.execute(query)

# Count occurrences of each date
date_counts = Counter(row.date for row in rows)

# Sort data by date for proper plotting
sorted_dates = sorted(date_counts.keys())
counts = [date_counts[date] for date in sorted_dates]

# Convert dates to strings for labeling
date_labels = [str(date) for date in sorted_dates]

# Create the bar chart
plt.figure(figsize=(15, 8))
plt.bar(date_labels, counts)
plt.xlabel('Date')
plt.ylabel('Weather Stations')
plt.title('Days where Snow is reported')
plt.xticks(rotation=90, fontsize=8)
plt.tight_layout()
plt.show()

# Close connection
cluster.shutdown()
