from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp import GcpCredentials

LOCAL_PATH = "/Users/dg/Downloads/dezoomcamp/7_project_Belgium_housing_market"

@task(retries=3)
def extract_from_gcs(file:str, year:int, month: int) -> Path:
    """Download trip data from GCS"""
    day = 31 if month in [3, 12] else 30
    date_encoding = f"{year}{month:02}{day}"

    gcs_path = f"{file}/{file}_{date_encoding}.csv"
    gcs_block = GcsBucket.load("dezoomcamp-gcs")
    local_path = Path(os.path.join(*[LOCAL_PATH, 'data', 'temp']))
    gcs_block.get_directory(
        from_path=gcs_path,
        local_path=local_path
    )

    return Path(f"{local_path}/{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning"""
    #df = pd.read_parquet(path)
    df = pd.read_csv(path)
    df = df[df['ParcelNature'] == "200"] # Maison/house
    df = df[df['TransactionType'] == "VENTEIMMEUB"] # Private direct sales
    date = f"{path}".split('/')[-1].split('.')[0].split('_')[-1]
    date = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
    df['Date'] = pd.Timestamp(date)
    # TODO something
    return df

@task()
def write_bq(file:str, df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("dezoomcamp-gcp-creds")
    df.to_gbq(
        destination_table=f"belgium_housing.{file}_all",
        project_id="dezoomcamp-green-taxi",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq(file = None, years = None, months = None) -> int:
    """Main ETL flow to load data into BigQuery"""

    path = extract_from_gcs(file, years, months)
    df = transform(path)
    write_bq(file, df)
    os.remove(path)
    return len(df)

@flow(log_prints=True)
def etl_gcs_to_bq_main(files = None, years = None, months = None) -> None:
    for year in years:
        for month in months:
            for file in files:
                etl_gcs_to_bq(file, year, month)

if __name__ == "__main__":
    months = [3, 6, 9, 12]
    years = list(range(2016,2023))
    #months = [3]
    #years = [2019]
    files = ['StatisticalUnitWideRealEstateTransactions',
             'ArrondissementWideRealEstateTransactions',
             'DivisionWideRealEstateTransactions',
             'MunicipalityWideRealEstateTransactions',
             'NationalWideRealEstateTransactions',
             'ProvincialWideRealEstateTransactions',
             'RegionalWideRealEstateTransactions'
             ]
    #base_hash = '89209670-51ca-11eb-beeb-3448ed25ad7c_20160331_csv_NA_01000'
    # name formatting 89209670-51ca-11eb-beeb-3448ed25ad7c_YYYYMMDD_csv_NA_01000
    # download link example https://opendata.fin.belgium.be/download/datasets/89209670-51ca-11eb-beeb-3448ed25ad7c_20160331_csv_NA_01000.zip
    etl_gcs_to_bq_main(files=files, years=years, months=months)

    