from datetime import datetime
import socket
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
    dates_to_extract = extraction_log_db.dates_to_upload()
    dates_to_extract = [item[0] for item in dates_to_extract]
    for date in dates_to_extract:
        db_path = "../loveletter/model/" + date.replace("-", "_") + "_" + "telemetry.db"
        if os.path.isfile(db_path) is False:
            extraction_log_db.set_uploaded_false(date)
            print(f"no data for {date}, manually check")
            continue
        try:
            upload_db(date, db_path)
        except Forbidden:
            print(f"data already uploaded for {date}") # aka db already in bucket
            extraction_log_db.set_uploaded_true(date)
        except TransportError:  # aka no internet connection
            print(f"no internet connection on {datetime.now().strftime('%m/%d/%Y}')},"
                  f"couldn't upload the file {db_path}")
        else:
            extraction_log_db.set_uploaded_true(date)
    extraction_log_db.close_connection()


def upload_db(date, local_file_path):
    client = storage.Client.from_service_account_json(CREDENTIALS_PATH[0])
    gcs_file_name = BOAT_NAME + "_" + date + "_" + "telemetry_data.db"
    bucket = client.bucket(BUCKET_NAME)
    # Create a blob (object) in the bucket
    blob = bucket.blob(gcs_file_name)
    blob.upload_from_filename(local_file_path)
    print(f"File {local_file_path} uploaded")


if __name__ == "__main__":
    main()
