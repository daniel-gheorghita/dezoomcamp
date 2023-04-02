import pandas as pd

data_filepath = '/Users/dg/Downloads/dezoomcamp/7_project_Belgium_housing_market/data/StatisticalUnitWideRealEstateTransactions_20160331.csv'
data_filepath = '/Users/dg/Downloads/dezoomcamp/7_project_Belgium_housing_market/data/20190331/StatisticalUnitWideRealEstateTransactions_20190331.csv'
for df in pd.read_csv(data_filepath,sep=';', chunksize=100):
    print(df.head())
    print(df.describe())
    print(df.dtypes)
    break

df = pd.read_csv(data_filepath,sep=';')
#print(df.describe())
df = df.dropna()
for str_col in ['NISCode', 'Fictious', 'NameFre', 'NameDut', 'NameGer', 'TransactionType', 'ParcelNature']:
    df[str_col] = df[str_col].astype('str')
print(df.head())
print(df.describe())
print(df.dtypes)
df.to_parquet(data_filepath + '.parquet', compression="gzip")


