import os
import logging
import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

S3_PATH = "s3://nyu-de-project-chris/processed/clean_ev_stations.parquet"
TABLE_NAME = "ev_stations"

SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

start_time = time.perf_counter()

try:
    logging.info("Starting Supabase load pipeline")

    if not SUPABASE_PASSWORD:
        raise ValueError("SUPABASE_PASSWORD environment variable is missing")

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
        username="postgres.tofqnmrnwlaoovjrpxbc",
        password=SUPABASE_PASSWORD,
        host="aws-1-us-east-2.pooler.supabase.com",
        port=5432,
        database="postgres",
        query={"sslmode": "require"}
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
        method="multi"
    )

    write_end = time.perf_counter()
    logging.info(f"Upload completed in {write_end - write_start:.2f} seconds")

    total_time = time.perf_counter() - start_time
    logging.info("Pipeline finished successfully")
    logging.info(f"Total runtime: {total_time:.2f} seconds")
    logging.info(f"Throughput: {len(df) / total_time:.2f} rows/sec")

except Exception:
    logging.error("Pipeline failed", exc_info=True)
    raise