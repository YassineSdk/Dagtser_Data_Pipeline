from dagster import EnvVar 
from dagster_snowflake import SnowflakeResource 
from dotenv import load_dotenv 

load_dotenv(".env")
Sf_ressource = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    database=EnvVar("SNOWFLAKE_DATABASE"),
    schema=EnvVar("SNOWFLAKE_SCHEMA"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
    role=EnvVar("SNOWFLAKE_ROLE"),
) 
