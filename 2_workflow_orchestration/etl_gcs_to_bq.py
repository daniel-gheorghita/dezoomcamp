from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color:str, year:int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("dezoomcamp-gcs")
    local_path = Path(f"/Users/dg/Downloads/dezoomcamp/data")
    gcs_block.get_directory(
        from_path=gcs_path,
        local_path=local_path
    )

    return Path(f"{local_path}/{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning"""
    df = pd.read_parquet(path)
    print(f"pre:missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post:missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("dezoomcamp-gcp-creds")
    df.to_gbq(
        destination_table="yellow_trips_data.rides",
        project_id="dezoomcamp-green-taxi",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq(color:str, year:int, month:int) -> int:
    """Main ETL flow to load data into BigQuery"""

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    return len(df)

@flow(log_prints=True)
def etl_gcs_to_bq_main(color:str, year:int, months: list[int]) -> None:
    total_lines_processed = 0
    for month in months:
        lines_processed = etl_gcs_to_bq(color=color, year=year, month=month)
        total_lines_processed += lines_processed

    print(f"Total rows processed: {total_lines_processed}")

if __name__ == "__main__":
    months = [2, 3]
    year = 2019
    color = "yellow"
    etl_gcs_to_bq_main(color=color, year=year, months=months)

    # Q3: 14851920