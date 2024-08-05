from datetime import datetime
import sqlite3
import socket
import pandas as pd
from fastparquet import write
from google.cloud import storage
import os
import glob
from extraction_log import ExtractionLogDb
from google.api_core.exceptions import Forbidden
from google.auth.exceptions import TransportError


BOAT_NAME = socket.gethostname()
CREDENTIALS_PATH = glob.glob(os.path.join("keys/", '*.json'))
BUCKET_NAME = open('keys/bucket.txt', 'r').readline()


def main():
    extraction_log_db = ExtractionLogDb()
    dates_to_convert = extraction_log_db.dates_to_convert()
    dates_to_convert = [item[0] for item in dates_to_convert]
    for date in dates_to_convert:
        db_path = "../loveletter/model/" + date.replace("-", "_") + "_" + "telemetry.db"
        if os.path.isfile(db_path) is False:
            extraction_log_db.set_date_false(date)
            print(f"no data for {date}, manually check")
            continue
        file_path = "extracted_files/" + date.replace("-", "_") + "_" + "telemetry_data.parquet"
        if os.path.isfile(file_path):  # if already converted then next iteration.
            extraction_log_db.set_parquet_converted_true(date)
        try:
            telemetry_results, col_names = get_telemetry(db_path)
        except ValueError:
            print(f"no data for {date}, manually check")
        else:
            parquet_writer(telemetry_results, col_names, date)
            extraction_log_db.set_parquet_converted_true(date)
    dates_to_upload = extraction_log_db.dates_to_upload()
    dates_to_upload = [item[0] for item in dates_to_upload]
    for date in dates_to_upload:
        local_file_path = "extracted_files/" + date.replace("-", "_") + "_" + "telemetry_data.parquet"
        try:
            upload_parquet(date, local_file_path)
        except Forbidden:
            print(f"data already uploaded for {date}")
            extraction_log_db.set_uploaded_true(date)
        except TransportError:  # aka no internet connection
            print(f"no internet connection on {datetime.now().strftime('%m/%d/%Y}')},"
                  f"couldn't upload the file {local_file_path}")
        else:
            extraction_log_db.set_uploaded_true(date)
    extraction_log_db.close_connection()


def get_telemetry(db_path):
    con = sqlite3.connect(db_path)
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
    cur.execute(query, (BOAT_NAME,))
    results = cur.fetchall()
    if not results:
        raise ValueError("No data found.")
    column_names = [desc[0] for desc in cur.description]
    con.close()
    return results, column_names


def parquet_writer(results, columns, date):
    df = pd.DataFrame(results, columns=columns)
    df = df.astype(str)  # fast parquet does not have explicit schema definition,
    # therefore I need to ensure that all the parquet files in bucket will be the same data types every time.
    # Pyarrow is not an option bc the version before 14.0 has a major security
    # concern that allows remote code execution.
    # There aren't wheels for pyarrow for raspberry pi and even less for rpi 3 32 bits.
    local_file_path = "extracted_files/" + date.replace("-", "_") + "_" + "telemetry_data.parquet"
    write(local_file_path, df)


def upload_parquet(date, local_file_path):
    client = storage.Client.from_service_account_json(CREDENTIALS_PATH[0])
    gcs_file_name = BOAT_NAME + "_" + date + "telemetry_data.parquet"
    bucket = client.bucket(BUCKET_NAME)
    # Create a blob (object) in the bucket
    blob = bucket.blob(gcs_file_name)
    blob.upload_from_filename(local_file_path)
    print(f"File {local_file_path} uploaded")


if __name__ == "__main__":
    main()
