from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os
from prefect.tasks import task_input_hash
from datetime import timedelta
import requests
from zipfile import ZipFile
import shutil
import numpy as np

LOCAL_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

@task(log_prints=True)
def download_url(url: str, save_path: Path, chunk_size: int=128) -> None:
    '''Download file from URL'''
    r = requests.get(url, stream=True)
    if not r.ok:
        print(f"Request for url {url} was not successful.")
        return
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

@task(log_prints=True)
def extract_zip(filepath: Path, output_folder: Path) -> None:
    '''Extract a zip file to a destination folder'''
    with ZipFile(filepath, 'r') as zObject:
        # Extracting all the members of the zip 
        # into a specific location.
        zObject.extractall(path=output_folder)

@task(log_prints=True)
def clean_zip_and_folder(filepath_zip: Path, extracted_zip_folder: Path) -> None:
    '''Remove ZIP file and extracted folder'''
    os.remove(filepath_zip)
    shutil.rmtree(extracted_zip_folder)

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_file: Path) -> pd.DataFrame:
    """Read housing data from web into pandas DataFrame"""
    dtype_dict = {'NISCode': str,
    'Fictious':             str,
    'NameFre':             str,
    'NameDut':             str,
    'NameGer':             str,
    'TransactionType':     str,
    'ParcelNature':         str,
    'ParcelsNumber':      np.float64,
    'PriceP25':           np.float64,
    'PriceP50':           np.float64,
    'PriceP75':           np.float64,
    'ParcelsAreaP25':     np.float64,
    'ParcelsAreaP50':     np.float64,
    'ParcelsAreaP75':     np.float64}
    df = pd.read_csv(dataset_file,sep=';')
    print(df.describe())
    return df

@task(log_prints=True)
def load_raw_clean_save(load_from = None, save_to = None, chunksize=10000):
    """Load big CSV file by chunks, remove rows with nans and save the reduced file to CSV."""
    header = True
    for df in pd.read_csv(load_from,sep=';', chunksize=10000):
        df = df.dropna()
        # NISCode;NameFre;NameDut;NameGer;RegistrationType;LessorType;TakerType;RentsNumber;RentP25;RentP50;RentP75;ChargesP25;ChargesP50;ChargesP75;TotalRentP25;TotalRentP50;TotalRentP75
        for str_col in ['NISCode', 'Fictious', 'NameFre', 'NameDut', 'NameGer', 'TransactionType', 'ParcelNature', 'RegistrationType', 'LessorType', 'TakerType']:
            try:
                df[str_col] = df[str_col].astype('str')
            except:
                #print(f"Column {str_col} is not present in this dataframe.")
                pass
        #df.to_parquet(dataset_file_parquet, compression="gzip", header=header, mode='a')
        df.to_csv(save_to, header=header, mode='a')
        header = False

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues and remove lines with NAN"""
    df = df.dropna()
    for str_col in ['NISCode', 'Fictious', 'NameFre', 'NameDut', 'NameGer', 'TransactionType', 'ParcelNature']:
        try:
            df[str_col] = df[str_col].astype('str')
        except:
            print(f"Field {str_col} is not present in this dataframe.")

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file:Path) -> Path:
    """Write dataframe out as parquet file"""

    print(f"Saving local file to {dataset_file}...")
    #Path("../data").mkdir(parents=True, exist_ok=True)
    df.to_parquet(dataset_file, compression="gzip")
    print(f"Saved local file to {dataset_file}")
    return dataset_file

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("belgium-housing-gcs")
    path_gcs_list = f"{path}".split('/')
    path_gcs = Path('/'.join([path_gcs_list[-3], path_gcs_list[-2], path_gcs_list[-1]]))
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=path_gcs
    )

@flow()
def etl_web_to_gcs(action_type, files, year, month) -> None:
    """The main ETL function"""

    day = 31 if month in [3, 12] else 30
    date_encoding = f"{year}{month:02}{day}"

    if action_type == "leases":
        base_url = "https://opendata.fin.belgium.be/download/datasets/84d5f470-51ca-11eb-8a67-3448ed25ad7c"
    elif action_type == "transactions":
        base_url = "https://opendata.fin.belgium.be/download/datasets/89209670-51ca-11eb-beeb-3448ed25ad7c"

    dataset_url = f"{base_url}_{date_encoding}_csv_NA_01000.zip"
    base_output_folder = Path(os.path.join(*[LOCAL_PATH, 'data', action_type]))
    filepath_zip = Path(os.path.join(base_output_folder, f"{date_encoding}.zip"))
    folder_extracted_zip = Path(os.path.join(base_output_folder, f"{date_encoding}"))


    for file in files:
        # Skip if the action_type does not match the file naming
        if action_type == "leases" and "Transactions" in file:
            continue
        if action_type == "transactions" and "Rents" in file:
            continue
        dataset_file = Path(os.path.join(*[base_output_folder, date_encoding, f"{file}_{date_encoding}.csv"]))
        output_folder = Path(os.path.join(base_output_folder, f"{file}"))
        dataset_file_parquet = Path(os.path.join(output_folder, f"{file}_{date_encoding}.parquet"))
        dataset_file_csv = Path(os.path.join(output_folder, f"{file}_{date_encoding}.csv"))
        os.makedirs(output_folder, exist_ok=True)
        if os.path.exists(dataset_file_parquet):
            print(f"{dataset_file_parquet} was already generated.")
        else:
            # Get the archive/folder in order to generate the parquet file
            if not os.path.exists(folder_extracted_zip):
                if not os.path.exists(filepath_zip):
                    download_url(dataset_url, filepath_zip)

                extract_zip(filepath_zip, folder_extracted_zip)
                # Clean storage (remove downloaded ZIP file)
                os.remove(filepath_zip)

            #df = fetch(dataset_file)
            #df = pd.read_csv(dataset_file,sep=';')

            if not os.path.exists(dataset_file):
                # Not all datasets have the same atomic representation files
                print(f'Dataset file {dataset_file} does not exist in this folder.')
                continue

            if not os.path.exists(dataset_file_csv):
                # Load the large raw CSV file, remove the NAN rows and save the result as CSV
                load_raw_clean_save(load_from = dataset_file, save_to = dataset_file_csv, chunksize=10000)

            #df = clean(df)

            if not os.path.exists(dataset_file_parquet):
                # Read the CSV file without NAN rows
                df = fetch(dataset_file_csv)

                # Clean storage (remove CSV file without NANs; parquet will be saved instead)
                #os.remove(dataset_file_csv)

                # Write the parquet file

                path = write_local(df, dataset_file_parquet)

        write_gcs(dataset_file_parquet)
        write_gcs(dataset_file_csv)
    
    if os.path.exists(folder_extracted_zip):
        # Clean storage (remove folder of the extracted ZIP)
        shutil.rmtree(folder_extracted_zip)

@flow(log_prints=True)
def etl_web_to_gcs_main(action_types = None, files = None, years = None, months = None) -> None:
    for action_type in action_types:
        for year in years:
            for month in months:
                etl_web_to_gcs(action_type, files, year, month)

if __name__ == "__main__":
    action_types = ["transactions", "leases"]
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
             'RegionalWideRealEstateTransactions',
            'ArrondissementWideRealEstateRents',  
            'ProvincialWideRealEstateRents',     
            'RegionalWideRealEstateRents',
            'MunicipalityWideRealEstateRents',    
            'NationalWideRealEstateRents'
             ]
    #base_hash = '89209670-51ca-11eb-beeb-3448ed25ad7c_20160331_csv_NA_01000'
    # name formatting for sales 89209670-51ca-11eb-beeb-3448ed25ad7c_YYYYMMDD_csv_NA_01000
    # name formatting for leases 84d5f470-51ca-11eb-8a67-3448ed25ad7c_YYYYMMDD_csv_NA_01000.zip
    # download link example https://opendata.fin.belgium.be/download/datasets/89209670-51ca-11eb-beeb-3448ed25ad7c_20160331_csv_NA_01000.zip
    etl_web_to_gcs_main(action_types=action_types, files=files, years=years, months=months)
