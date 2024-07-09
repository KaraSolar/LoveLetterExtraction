from datetime import datetime, timedelta
import sqlite3
import socket
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import os
import glob


boat_name = socket.gethostname()
date_time = datetime.now()
yesterday_date = (date_time - timedelta(days=1)).strftime("%Y_%m_%d_")
path = "../loveletter/model/" + yesterday_date + "telemetry.db"
credentials_path = glob.glob(os.path.join("keys/", '*.json'))
bucket_name = open('keys/bucket.txt', 'r').readline()

con = sqlite3.connect(path)
cur = con.cursor()

query = '''SELECT 
       ? AS Boat,
       telemetryId,
       telemetryTimeStamp,
       TelemetryData.tripId,
       telemetryBatteryVoltageSystem,
       telemetryBatteryCurrentSystem,
       telemetryBatteryPowerSystem,
       telemetryBatteryStateOfChargeSystem,
       telemetryPVDCCoupledPower,
       telemetryPVDCCoupledCurrent,
       telemetryLatitude1,
       telemetryLatitude2,
       telemetryLongitude1,
       telemetryLongitude2,
       telemetryCourse,
       telemetrySpeed,
       telemetryGPSFix,
       telemetryGPSNumberOfSatellites,
       telemetryAltitude1,
       telemetryAltitude2,
       Trip.tripPassengerQty
  FROM TelemetryData
  LEFT JOIN Trip 
      ON TelemetryData.tripId = Trip.tripId;'''

cur.execute(query, (boat_name,))
results = cur.fetchall()

if not results:
    # log
    raise ValueError("No data found.")

cur.close()
con.close()


schema = pa.schema([
    ('Boat', pa.string()),
    ('telemetryId', pa.int64()),
    ('telemetryTimeStamp', pa.string()),  # Change the schema to date type
    ('tripId', pa.int64()),
    ('telemetryBatteryVoltageSystem', pa.float64()),
    ('telemetryBatteryCurrentSystem', pa.float64()),
    ('telemetryBatteryPowerSystem', pa.int64()),
    ('telemetryBatteryStateOfChargeSystem', pa.int64()),
    ('telemetryPVDCCoupledPower', pa.int64()),
    ('telemetryPVDCCoupledCurrent', pa.float64()),
    ('telemetryLatitude1', pa.int64()),
    ('telemetryLatitude2', pa.int64()),
    ('telemetryLongitude1', pa.int64()),
    ('telemetryLongitude2', pa.int64()),
    ('telemetryCourse', pa.int64()),
    ('telemetrySpeed', pa.float64()),
    ('telemetryGPSFix', pa.int64()),
    ('telemetryGPSNumberOfSatellites', pa.int64()),
    ('telemetryAltitude1', pa.int64()),
    ('telemetryAltitude2', pa.int64()),
    ('tripPassengerQty', pa.int64())
])

table = pa.Table.from_pydict({cur.description[i][0]: [row[i] for row in results] for i in range(len(cur.description))}, schema=schema)

local_file_path = "extracted_files/" + yesterday_date + "telemetry_data.parquet"
pq.write_table(table, local_file_path)

client = storage.Client.from_service_account_json(credentials_path[0])

gcs_file_name = boat_name + yesterday_date + "telemetry_data.parquet"

bucket = client.bucket(bucket_name)

# Create a blob (object) in the bucket
blob = bucket.blob(gcs_file_name)

blob.upload_from_filename(local_file_path)

print(f"File {local_file_path} uploaded to {bucket_name} as {gcs_file_name}")