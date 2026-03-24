from dagster import asset, AssetExecutionContext as ASC 
from dagster_snowflake import SnowflakeResource 
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd 
import polars as pl
from datetime import datetime , timedelta
import scipy.stats as stats 


# loading the full daily data 
@asset(
    group_name="Aggregation",
    description="Load yesterday's clean data from snowflake",
    tags={"kind":"aggregation"})

def Loading_daily_data(context: ASC,
    Sf_ressource:SnowflakeResource) -> pl.DataFrame:

    #yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    yesterday = datetime.now()
    query = f"""
        SELECT * FROM CLEAN_DATA
        WHERE DATE = '{yesterday}'
    """
    with Sf_ressource.get_connection() as conn:
        df = pd.read_sql(query, conn)
    context.log.info(f"Loaded {len(df)} row for { yesterday}")
    
    return (
        pl.from_pandas(df)
        .with_columns([
            pl.col("PERCENTAGEOFBASELINE").cast(pl.Float64, strict=False)
            ])
        )


# first level aggregation of the data 
@asset(
    group_name="Aggregation",
    description="Aggregate by date and station — avg, std, and 95% confidence interval",
    tags={"kind":"aggregation"})
def aggregating_data(context: ASC,
    Loading_daily_data: pl.DataFrame) -> pl.DataFrame:
    agg_df = Loading_daily_data
    agg_df = agg_df.group_by(["DATE","STATION"]).agg([
        pl.col("PERCENTAGEOFBASELINE").mean().alias("avg_crowding"),
        pl.col("PERCENTAGEOFBASELINE").std().alias("std_crowding"),
        pl.col("PERCENTAGEOFBASELINE").count().alias("sample_count"),
    ])
    agg_df = agg_df.with_columns([
        (pl.col("avg_crowding") - 1.96 * (pl.col("std_crowding") / pl.col("sample_count").sqrt()))
            .alias("ci_lower"),
        (pl.col("avg_crowding") + 1.96 * (pl.col("std_crowding") / pl.col("sample_count").sqrt()))
            .alias("ci_upper"),
        (pl.col("DATE").cast(pl.Utf8))
    
    ])
    context.log.info(f"aggregated{agg_df.shape[0]} station-day rows")
    context.log.info(f"data preview {agg_df.head()}")
    return agg_df


# Append the yeasterday aggregation to AGGREGATED_DATA
@asset(
    group_name="Aggregation",
    description="Append yesterday's aggregation to AGGREGATED_DATA table in Snowflake",
    tags={"kind": "aggregation"}
)
def load_aggregated_data(
    context: ASC,
    aggregating_data: pl.DataFrame,
    Sf_ressource: SnowflakeResource
) -> None:
    df = aggregating_data.to_pandas()
    df.columns = [col.upper() for col in df.columns]
    #df['DATE'] = pd.to_datetime( df['DATE'])

    with Sf_ressource.get_connection() as conn:
        write_pandas(
            conn=conn,
            df=df,
            table_name="AGGREGATED_DATA",
            auto_create_table=True,
            overwrite=False
        )
    context.log.info(f"Appended {df.shape[0]} rows to AGGREGATED_DATA")









