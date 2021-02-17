import geojson
from google.cloud import bigquery
from datetime import datetime
import requests

bigquery_client = bigquery.Client()

# This example uses a table containing a column named "geo" with the
# GEOGRAPHY data type.
table_id = "primeval-aspect-151903.gps_data.location"

# Get GPS position
response = requests.get("https://web.trafi.com/api/realtime-vehicles/rio?scheduleId=brrim_TRO-2_20200610&trackId=a-b")
data = response.json()["mapMarkers"]

# Use the python-geojson library to generate GeoJSON of a line from LAX to
# JFK airports. Alternatively, you may define GeoJSON data directly, but it
# must be converted to a string before loading it into BigQuery.
rows = []
for bus in data:
    row = {}
    row["bus_id"] = bus["id"]
    geo = geojson.Point((bus["coordinate"]["lng"], bus["coordinate"]["lat"]))
    row["geo"] = geojson.dumps(geo)
    row["timestamp"] = datetime.now().isoformat()
    rows.append(row)

#  table already exists and has a column
# named "geo" with data type GEOGRAPHY.
errors = bigquery_client.insert_rows_json(table_id, rows)
if errors:
    raise RuntimeError(f"row insert failed: {errors}")
else:
    print(f"wrote %d row to {table_id}" % len(rows))