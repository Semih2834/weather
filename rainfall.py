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

# Query to fetch date data from rainy_days table
query = "SELECT date FROM rainy_days"
rows = session.execute(query)

# Count occurrences of each date
date_counts = Counter(row.date for row in rows)

# Sort data by date for proper plotting
sorted_dates = sorted(date_counts.keys())
counts = [date_counts[date] for date in sorted_dates]

# Split the data into two parts
mid_index = len(sorted_dates) // 2
dates_part1, dates_part2 = sorted_dates[:mid_index], sorted_dates[mid_index:]
counts_part1, counts_part2 = counts[:mid_index], counts[mid_index:]

# Convert dates to strings for labeling
labels_part1 = [str(date) for date in dates_part1]
labels_part2 = [str(date) for date in dates_part2]

# Create the split bar chart
fig, axes = plt.subplots(2, 1, figsize=(15, 12), sharey=True)

# First subplot
axes[0].bar(labels_part1, counts_part1)
axes[0].set_title('Days where rain is reported (Part 1)')
axes[0].set_ylabel('Weather Stations')
axes[0].tick_params(axis='x', rotation=90, labelsize=8)

# Second subplot
axes[1].bar(labels_part2, counts_part2)
axes[1].set_title('Days where rain is reported(Part 2)')
axes[1].set_xlabel('Date')
axes[1].set_ylabel('Weather Stations')
axes[1].tick_params(axis='x', rotation=90, labelsize=8)

# Adjust layout for readability
plt.tight_layout()
plt.show()

# Close connection
cluster.shutdown()

