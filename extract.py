from datetime import datetime, timedelta
import sqlite3
import socket
import pandas as pd
from fastparquet import write
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

column_names = [desc[0] for desc in cur.description]
df = pd.DataFrame(results, columns=column_names)
df = df.astype(str) # fast parquet does not have explicit schema definition, 
 # therefore I need to ensure that all of the parquet files in bucket will be the same data types every time.
 # Pyarrow is not an option bc the version before 14.0 has a major security concern that allows remote code execution.
 # There are not wheels for pyarrow for raspberry pi and even less for rpi 3 32 bits.

cur.close()
con.close()


local_file_path = "extracted_files/" + yesterday_date + "telemetry_data.parquet"
write(local_file_path, df)

client = storage.Client.from_service_account_json(credentials_path[0])

gcs_file_name = boat_name + "_" + yesterday_date + "telemetry_data.parquet"

bucket = client.bucket(bucket_name)

# Create a blob (object) in the bucket
blob = bucket.blob(gcs_file_name)

blob.upload_from_filename(local_file_path)

print(f"File {local_file_path} uploaded to {bucket_name} as {gcs_file_name}")
