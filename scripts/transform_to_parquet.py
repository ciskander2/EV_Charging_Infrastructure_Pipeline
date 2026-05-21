import json
import logging
import os
import time
import boto3
import pandas as pd

BUCKET_NAME = os.getenv("S3_BUCKET")
RAW_PREFIX = os.getenv("RAW_PREFIX", "raw/ev_stations").strip("/") + "/"
PROCESSED_KEY = os.getenv("PROCESSED_KEY", "processed/clean_ev_stations.parquet")
LOCAL_OUTPUT_PATH = os.getenv("LOCAL_PARQUET_PATH", "/tmp/clean_ev_stations.parquet")

s3 = boto3.client("s3")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_s3_object_with_retry(bucket, key, retries=5):
    for attempt in range(retries):
        try:
            return s3.get_object(Bucket=bucket, Key=key)
        except Exception as e:
            wait = 2 ** attempt
            logging.warning(f"Error reading {key}. Retrying in {wait}s. Error: {e}")
            time.sleep(wait)

    raise Exception(f"Failed to read {key} after {retries} retries")


def first_present(source, *keys):
    for key in keys:
        value = source.get(key)
        if value is not None:
            return value
    return None


def normalize_station(station):
    """Support both NREL API records and the nested sample JSON in this repo."""
    address = station.get("address") or {}
    coordinates = station.get("coordinates") or {}
    network = station.get("network") or {}
    access = station.get("access") or {}
    chargers = station.get("chargers") or {}

    return {
        "station_id": station.get("id"),
        "station_name": station.get("station_name"),
        "city": first_present(station, "city") or address.get("city"),
        "state": first_present(station, "state") or address.get("state"),
        "latitude": first_present(station, "latitude") or coordinates.get("latitude"),
        "longitude": first_present(station, "longitude") or coordinates.get("longitude"),
        "network": first_present(station, "ev_network") or network.get("provider"),
        "level2_ports": first_present(
            {"api": first_present(station, "ev_level2_evse_num"), **chargers},
            "api",
            "level2_ports",
        ),
        "dc_fast_ports": first_present(
            {"api": first_present(station, "ev_dc_fast_num"), **chargers},
            "api",
            "dc_fast_ports",
        ),
        "access": first_present(station, "access_code") or access.get("access_code"),
        "status": station.get("status_code"),
    }


def main():
    start_time = time.perf_counter()

    logging.info("Starting S3 transform pipeline")

    if not BUCKET_NAME:
        raise ValueError("S3_BUCKET environment variable is missing")

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

            if isinstance(stations, dict):
                stations = [stations]

            for station in stations:
                rows.append(normalize_station(station))

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("No records loaded from S3 raw layer")

    logging.info(f"Loaded {len(df)} records from S3")
    logging.info(f"Initial dataset shape: {df.shape}")

    logging.info(f"Missing level2_ports: {df['level2_ports'].isna().sum()}")
    logging.info(f"Missing dc_fast_ports: {df['dc_fast_ports'].isna().sum()}")

    df["level2_ports"] = df["level2_ports"].fillna(0).astype(int)
    df["dc_fast_ports"] = df["dc_fast_ports"].fillna(0).astype(int)
    df["station_count"] = 1
    df["infrastructure_score"] = df["level2_ports"] + (df["dc_fast_ports"] * 20)
    df["fast_charging_density"] = df["dc_fast_ports"] / df["station_count"]

    logging.info("Handled missing values and type casting")

    logging.info(f"Final rows written to Parquet: {len(df):,}")
    logging.info(f"Final columns written to Parquet: {df.shape[1]}")
    df.to_parquet(LOCAL_OUTPUT_PATH, index=False)
    logging.info(f"Saved local Parquet file: {LOCAL_OUTPUT_PATH}")

    s3.upload_file(
        LOCAL_OUTPUT_PATH,
        BUCKET_NAME,
        PROCESSED_KEY
    )

    logging.info(f"Uploaded processed data to s3://{BUCKET_NAME}/{PROCESSED_KEY}")

    end_time = time.perf_counter()
    logging.info(f"S3 transform pipeline completed in {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.error("Pipeline failed", exc_info=True)
        raise
