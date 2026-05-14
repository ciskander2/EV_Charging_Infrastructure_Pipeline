import json
import logging
import time
import boto3
import pandas as pd

BUCKET_NAME = "nyu-de-project-chris"
RAW_PREFIX = "raw/ev_stations/"
PROCESSED_KEY = "processed/clean_ev_stations.parquet"

s3 = boto3.client("s3")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

start_time = time.perf_counter()


def get_s3_object_with_retry(bucket, key, retries=5):
    for attempt in range(retries):
        try:
            return s3.get_object(Bucket=bucket, Key=key)
        except Exception as e:
            wait = 2 ** attempt
            logging.warning(f"Error reading {key}. Retrying in {wait}s. Error: {e}")
            time.sleep(wait)

    raise Exception(f"Failed to read {key} after {retries} retries")


try:
    logging.info("Starting S3 transform pipeline")

    rows = []

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=RAW_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".json"):
                continue

            logging.info(f"Reading s3://{BUCKET_NAME}/{key}")

            file_obj = get_s3_object_with_retry(BUCKET_NAME, key)
            stations = json.loads(file_obj["Body"].read())

            for station in stations:
                rows.append({
                    "station_id": station.get("id"),
                    "station_name": station.get("station_name"),
                    "city": station.get("city"),
                    "state": station.get("state"),
                    "latitude": station.get("latitude"),
                    "longitude": station.get("longitude"),
                    "network": station.get("ev_network"),
                    "level2_ports": station.get("ev_level2_evse_num"),
                    "dc_fast_ports": station.get("ev_dc_fast_num"),
                    "access": station.get("access_code"),
                    "status": station.get("status_code")
                })

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("No records loaded from S3 raw layer")

    logging.info(f"Loaded {len(df)} records from S3")
    logging.info(f"Initial dataset shape: {df.shape}")

    logging.info(f"Missing level2_ports: {df['level2_ports'].isna().sum()}")
    logging.info(f"Missing dc_fast_ports: {df['dc_fast_ports'].isna().sum()}")

    df["level2_ports"] = df["level2_ports"].fillna(0).astype(int)
    df["dc_fast_ports"] = df["dc_fast_ports"].fillna(0).astype(int)

    logging.info("Handled missing values and type casting")

    local_file = "clean_ev_stations.parquet"
    logging.info(f"Final rows written to Parquet: {len(df):,}")
    logging.info(f"Final columns written to Parquet: {df.shape[1]}")
    df.to_parquet(local_file, index=False)
    logging.info(f"Saved local Parquet file: {local_file}")

    s3.upload_file(
        local_file,
        BUCKET_NAME,
        PROCESSED_KEY
    )

    logging.info(f"Uploaded processed data to s3://{BUCKET_NAME}/{PROCESSED_KEY}")

    end_time = time.perf_counter()
    logging.info(f"S3 transform pipeline completed in {end_time - start_time:.2f} seconds")

except Exception:
    logging.error("Pipeline failed", exc_info=True)