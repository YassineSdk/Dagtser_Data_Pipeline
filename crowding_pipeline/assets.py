from utils.get_raw_data import get_raw_data
from dagster import asset , AssetExecutionContext as ASC 
from dagster_snowflake import SnowflakeResource
from snowflake.connector.pandas_tools import write_pandas
import polars as pl
import pandas as pd


## fetching the data from the API
@asset(
    group_name="ingestion",
    description="Live crowding data from TFL APi for landing station",
    tags={"kind":"Raw"}
)
def raw_crowding_data(context: ASC, ) -> pd.DataFrame:

    data , len_data ,available_data = get_raw_data()
    context.log.info(f" Total records fetched : {len_data}")
    context.log.info(f" Records with available crowding data: {available_data}")
    return data


# loading the raw data to the raw db 
@asset(
    group_name="Loading_raw",
    description="Loading raw data into the raw db in the snowflake warehouse",
    tags={"kind":"Loading_Raw"}
)
def load_raw_data(context:ASC,raw_crowding_data,Sf_ressource:SnowflakeResource)->None:

    data = raw_crowding_data
    data.columns = [col.upper() for col in data.columns]
    with Sf_ressource.get_connection() as conn:
        write_pandas(
            conn=conn,
            df=data,
            table_name="RAW_Data",
            auto_create_table=True,
            overwrite=False
        )
    context.log.info(f"size of loaded data {data.shape}")


# cleaning the raw data droping Nan values ..
@asset(
    group_name="cleaning",
    description=" data cleaning and processing of the ingested data",
    tags={"kind":"Cleaning"})

def cleaned_crowding_data(
    context:ASC,
    raw_crowding_data: pd.DataFrame )-> pl.DataFrame:

    df = pl.from_pandas(raw_crowding_data)
    df = df.filter(pl.col("dataAvailable"))
    df = df.drop_nulls().unique()
    context.log.info(f"Data shape: {df.shape}")
    context.log.info(f"Data schema: {df.schema}")
    return df


# processing the data by transforming the date col to datetime and dropping irelevant data
@asset(
    group_name="processing",
    description="transforming col data types and fixing  and dropping irelevant columns"
)
def processed_data(
    context:ASC,
    cleaned_crowding_data: pl.DataFrame )-> pl.DataFrame:
    df = cleaned_crowding_data
    df = df.select('percentageOfBaseline', 'timeLocal', 'station')
    df = df.with_columns([
        pl.col("timeLocal").str.to_datetime(format="%Y-%m-%d %H:%M:%S")
    ])
    context.log.info(f"Data columns: {df.columns}")
    return df


# creating date feature
@asset(
    group_name="feature_engineering",
    description="creating new columns like day , month, year",
    tags = {"kind":"Feature_engineering"}
)
def feature_engineering(
    context:ASC,
    processed_data: pl.DataFrame)-> pd.DataFrame:
    df = processed_data.to_pandas()
    df['timeLocal'] = df['timeLocal'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['date'] = pd.to_datetime(df['timeLocal']).dt.date
    context.log.info(f"new columns preview : {df.head()}")
    context.log.info(f"number of columns : {df.info()}")
    return df


# storing Loading the clean data enriched data 
@asset(group_name="loading_clean_data",
    description="Loading clean data to the warehouse for aggregation",
    tags={"kind":"Storing_clean_Data"})

def loading_clean_data(
    context: ASC,
    feature_engineering: pd.DataFrame,
    Sf_ressource: SnowflakeResource )-> None :
    data =  feature_engineering
    data.columns = [col.upper() for col in data.columns]
    with Sf_ressource.get_connection() as conn:
        write_pandas(
            conn=conn,
            df=data,
            table_name="CLEAN_DATA",
            auto_create_table=True,
            overwrite=False
        )
    context.log.info(f"data overview : {data.head()}")
    context.log.info(f"size of ingested data is : {data.shape}")


















