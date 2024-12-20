import requests
import pandas as pd
import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from datetime import datetime

# NOAA API Key
API_TOKEN = "uahsapVscmuHNQqfGSCCniyPTYWCQVYM"
BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2"
headers = {"token": API_TOKEN}

CASSANDRA_KEYSPACE = "project"
CASSANDRA_TABLE = "new_jersey_weather"
cloud_config= {
  'secure_connect_bundle': 'secure-connect-linkedin.zip'
}
with open("linkedin-token.json") as f:
    secrets = json.load(f)
CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]
auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)

# Function to connect to Cassandra
def connect_to_cassandra():
    
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(CASSANDRA_KEYSPACE)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} (
            station TEXT,
            date DATE,
            tmin FLOAT,
            tmax FLOAT,
            prcp FLOAT,
            snow FLOAT,
            wind FLOAT,
            humidity FLOAT,
            PRIMARY KEY (station, date)
        );
    """)
    return session

# Function to fetch weather data for a station
def get_weather_data(station_id, start_date, end_date, dataset="GHCND"):
    url = f"{BASE_URL}/data"
    all_data = []
    offset = 1
    while True:
        params = {
            "datasetid": dataset,
            "stationid": station_id,
            "startdate": start_date,
            "enddate": end_date,
            "limit": 1000,
            "offset": offset
        }
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            time.sleep(0.2)
            if response.status_code == 200:
                try:
                    data = response.json().get("results", [])
                    if not data:
                        break
                    all_data.extend(data)
                    offset += 1000
                except json.JSONDecodeError:
                    print(f"Invalid JSON for station {station_id}")
                    break
            else:
                print(f"Error fetching data for {station_id}: {response.text}")
                break
        except requests.RequestException as e:
            print(f"Request error for station {station_id}: {e}")
            break
    return pd.DataFrame(all_data)

# Function to pivot data and ensure proper columns
def pivot_weather_data(data):
    if data.empty:
        return pd.DataFrame()
    pivoted = data.pivot_table(
        index=["station", "date"],
        columns="datatype",
        values="value",
        aggfunc="mean"
    ).reset_index()
    required_columns = ["station", "date", "TMIN", "TMAX", "PRCP", "SNOW", "WIND", "HUMIDITY"]
    for col in required_columns:
        if col not in pivoted.columns:
            pivoted[col] = None
    # Convert date to YYYY-MM-DD format
    pivoted["date"] = pivoted["date"].apply(lambda x: x.split("T")[0])
    return pivoted[required_columns]

# Function to push data into Cassandra
def push_data_to_cassandra(session, data):
    if data.empty:
        print("No data to push to Cassandra.")
        return
    query = f"""
        INSERT INTO {CASSANDRA_TABLE} (station, date, tmin, tmax, prcp, snow, wind, humidity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """
    prepared = session.prepare(query)
    for _, row in data.iterrows():
        session.execute(prepared, (
            row["station"],
            datetime.strptime(row["date"], "%Y-%m-%d").date(),  # Convert to Cassandra-compatible date
            row["TMIN"],
            row["TMAX"],
            row["PRCP"],
            row["SNOW"],
            row["WIND"],
            row["HUMIDITY"]
        ))
    print(f"Pushed {len(data)} rows to Cassandra.")

# Main function to collect weather data for New Jersey
def gather_and_store_new_jersey_weather(start_date, end_date):
    url = f"{BASE_URL}/stations"
    params = {"locationid": "FIPS:34", "datasetid": "GHCND", "limit": 1000}
    try:
        stations_response = requests.get(url, headers=headers, params=params, timeout=10)
        time.sleep(0.2)
        stations = stations_response.json().get("results", [])
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"Error fetching stations: {e}")
        return

    if not stations:
        print("No stations found for New Jersey.")
        return

    session = connect_to_cassandra()
    for station in stations:
        station_id = station["id"]
        print(f"Fetching data for station: {station_id}")
        raw_data = get_weather_data(station_id, start_date, end_date)
        if not raw_data.empty:
            processed_data = pivot_weather_data(raw_data)
            push_data_to_cassandra(session, processed_data)

# Execute the script
if __name__ == "__main__":
    start_date = "2022-01-01"
    end_date = "2022-12-31"
    gather_and_store_new_jersey_weather(start_date, end_date)
