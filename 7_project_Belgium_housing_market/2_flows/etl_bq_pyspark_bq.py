import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F
from prefect import flow, task
from prefect_gcp import GcpCredentials
import pandas as pd
import pandas_gbq as pdbq


@task(log_prints=True, retries=3)
def get_bq_dataframe(filename:str = None, action_type:str = None) -> pd.DataFrame:
    
    # Credentials block
    gcp_credentials_block = GcpCredentials.load("belgium-housing-gcp-cred")
  
    # Create the table access string
    table_string = ''
    if action_type == "lease":
        table_string = f'belgium_housing_leases.{filename}Rents_all'
    elif action_type == "transaction":
        table_string = f'belgium_housing_transactions.{filename}Transactions_all'
    
    # Download the dataframe
    df = pdbq.read_gbq(query_or_table=table_string,
                        project_id='belgium-housing-market',
                        credentials=gcp_credentials_block.get_credentials_from_service_account())

    # Remove duplicates
    df = df.drop_duplicates()

    return df

@task(log_prints=True, retries=3)
def pyspark_transform(leases_df:pd.DataFrame = None, transactions_df:pd.DataFrame = None, spark_master:str = "local[*]", verbose:bool = False) -> pd.DataFrame:
    
    #Create PySpark SparkSession
    spark = SparkSession.builder \
        .master(spark_master) \
        .appName("belgian_housing_buy_lease") \
        .getOrCreate()
    
    #Create PySpark DataFrame from Pandas
    leases_psdf = spark.createDataFrame(leases_df) 
    transactions_psdf = spark.createDataFrame(transactions_df)
    
    # Print schemas
    if verbose:
        leases_psdf.printSchema()
        transactions_psdf.printSchema()

    # Select certain columns
    leases_psdf = leases_psdf.select("NISCode", "NameFre", "NameDut", "NameGer", "RentP50", "Date", "RentsNumber")
    transactions_psdf = transactions_psdf.select("NISCode", "NameFre", "NameDut", "NameGer", "PriceP50", "Date", "ParcelsNumber")

    # Print schemas
    if verbose:
        leases_psdf.printSchema()
        transactions_psdf.printSchema()

    # Means
    leases_psdf = \
    leases_psdf \
        .groupBy("NISCode", "Date") \
        .agg(F.avg("RentP50").alias("RentP50"), \
             F.sum("RentsNumber").alias("RentsNumber"),\
             F.first("NameFre").alias("NameFre")) \
        #.filter(F.col("NISCode")==71002) \
        #.sort("Date") \
        #.show(truncate=False)

    transactions_psdf = \
        transactions_psdf \
            .groupBy("NISCode", "Date") \
            .agg(F.avg("PriceP50").alias("PriceP50"), \
                 F.sum("ParcelsNumber").alias("ParcelsNumber"),\
                 F.first("NameDut").alias("NameDut"),\
                 F.first("NameGer").alias("NameGer")) \
        #.filter(F.col("NISCode")==71002) \
        #.sort("Date") \
        #.show(truncate=False)
    
    # PySpark join multiple columns
    transactions_and_leases_psdf = \
        leases_psdf \
            .join(transactions_psdf, ["NISCode","Date"],"inner") \
            .sort("NISCode", "Date") \
            #.show()
    
    # Add column ratio price/rent
    transactions_and_leases_psdf = transactions_and_leases_psdf.withColumn('PriceToRentRatio',
                       transactions_and_leases_psdf["PriceP50"] / transactions_and_leases_psdf["RentP50"])
    
    if verbose:
        transactions_and_leases_psdf.show()

    # PySpark to Pandas DF
    transactions_and_leases_df = transactions_and_leases_psdf.toPandas()

    # Return result
    return transactions_and_leases_df

@task(log_prints=True, retries=3)
def upload_df_to_bq(df: pd.DataFrame = None, filename:str = None) -> None:
    
    # Get GCP credentials block
    gcp_credentials_block = GcpCredentials.load("belgium-housing-gcp-cred")
    
    # Upload dataframe to BQ
    df.to_gbq(
            destination_table=f"belgium_housing_transactions_rents.{filename}",
            project_id="belgium-housing-market",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            chunksize=500000,
            if_exists="replace")

@flow(log_prints=True)
def etl_bq_pyspark_bq(filename:str = None) -> None:

    # Get dataframes from BQ
    leases_df = get_bq_dataframe(filename, "lease")
    transactions_df = get_bq_dataframe(filename, "transaction")

    # Transform and create a joint dataframe
    transactions_and_leases_df = pyspark_transform(leases_df, transactions_df)

    # Upload Pandas df to BQ
    upload_df_to_bq(transactions_and_leases_df, filename)   

@flow(log_prints=True)
def etl_bq_pyspark_bq_main(files:list[str] = None) -> None:
    for filename in files:
        etl_bq_pyspark_bq(filename)

if __name__ == "__main__":
    '''
    files = [    'ArrondissementWideRealEstate',
                 'MunicipalityWideRealEstate',
                 'NationalWideRealEstate',
                 'ProvincialWideRealEstate',
                'RegionalWideRealEstate',
                'StatisticalUnitWideRealEstateTransactions',
                'DivisionWideRealEstateTransactions'
            ]
    '''
    files = ['MunicipalityWideRealEstate']
    etl_bq_pyspark_bq_main(files)
