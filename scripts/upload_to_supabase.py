import logging
import os
import time

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
PROCESSED_KEY = os.getenv("PROCESSED_KEY", "processed/clean_ev_stations.parquet")
S3_PATH = os.getenv("PROCESSED_S3_PATH")
TABLE_NAME = os.getenv("SUPABASE_TABLE", "ev_stations")

SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = int(os.getenv("SUPABASE_PORT", "5432"))
SUPABASE_DATABASE = os.getenv("SUPABASE_DATABASE", "postgres")
SUPABASE_SSLMODE = os.getenv("SUPABASE_SSLMODE", "require")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def require_env(name, value):
    if not value:
        raise ValueError(f"{name} environment variable is missing")


def main():
    start_time = time.perf_counter()
    logging.info("Starting Supabase load pipeline")

    global S3_PATH
    if not S3_PATH:
        require_env("S3_BUCKET", S3_BUCKET)
        S3_PATH = f"s3://{S3_BUCKET}/{PROCESSED_KEY}"

    require_env("SUPABASE_USER", SUPABASE_USER)
    require_env("SUPABASE_PASSWORD", SUPABASE_PASSWORD)
    require_env("SUPABASE_HOST", SUPABASE_HOST)

    logging.info(f"Reading Parquet from S3: {S3_PATH}")
    read_start = time.perf_counter()
    df = pd.read_parquet(S3_PATH)
    read_end = time.perf_counter()
    logging.info(f"Loaded {len(df)} rows in {read_end - read_start:.2f} seconds")

    if df.empty:
        raise ValueError("DataFrame is empty after loading from S3")

    logging.info("Connecting to Supabase database")
    connection_url = URL.create(
        drivername="postgresql+psycopg2",
        username=SUPABASE_USER,
        password=SUPABASE_PASSWORD,
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        database=SUPABASE_DATABASE,
        query={"sslmode": SUPABASE_SSLMODE},
    )

    engine = create_engine(connection_url)

    logging.info(f"Uploading data to table: {TABLE_NAME}")
    write_start = time.perf_counter()
    df.to_sql(
        TABLE_NAME,
        engine,
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi",
    )
    write_end = time.perf_counter()
    logging.info(f"Upload completed in {write_end - write_start:.2f} seconds")

    total_time = time.perf_counter() - start_time
    logging.info("Pipeline finished successfully")
    logging.info(f"Total runtime: {total_time:.2f} seconds")
    logging.info(f"Throughput: {len(df) / total_time:.2f} rows/sec")


if __name__ == "__main__":
    main()
