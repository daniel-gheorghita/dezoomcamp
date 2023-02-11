from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os
from prefect.tasks import task_input_hash
from datetime import timedelta

LOCAL_PATH = "/Users/dg/Downloads/dezoomcamp"

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    #df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    #df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file:str) -> Path:
    """Write dataframe out as parquet file"""

    path = Path(f"/Users/dg/Downloads/dezoomcamp/data/{dataset_file}.parquet")
    print(f"Current working directory: {os.getcwd()}.")
    print(f"Saving local file to {path}...")
    #Path("../data").mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="gzip")
    print(f"Saved local file to {path}")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("dezoomcamp-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=f"{path}".split('/')[-1]
    )

@flow()
def etl_web_to_gcs(color:str, year:int, month:int) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    #dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz"
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_file)
    write_gcs(path)
    return len(df)

@flow(log_prints=True)
def etl_web_to_gcs_parent(color:str, year:int, months:list[int]) -> None:
    for month in months:
        etl_web_to_gcs(color=color, year=year, month=month)

if __name__ == "__main__":
    color = "fhv"
    year = 2019
    months = list(range(1,13))
    etl_web_to_gcs_parent(color=color, year=year, months=months)