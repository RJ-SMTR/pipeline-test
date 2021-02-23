import geojson
from google.cloud import bigquery
from datetime import datetime
import requests
from dagster import solid, pipeline
import json

@solid
def add_data(context, table_name: str, bus_id: str):
    bigquery_client = bigquery.Client()

    # This example uses a table containing a column named "geo" with the
    # GEOGRAPHY data type.
    table_id = table_name

    # Get GPS position
    response = requests.get(f"https://web.trafi.com/api/realtime-vehicles/rio?scheduleId={bus_id}&trackId=a-b")
    data = response.json()["mapMarkers"]
    context.log.debug(f"Received data: {json.dumps(data)}")

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
        context.log.error(f"row insert failed: {errors}")
        raise RuntimeError(f"row insert failed: {errors}")
    else:
        context.log.info(f"wrote %d row to {table_id}" % len(rows))

@pipeline
def add_data_pipeline():
    add_data()