from dagster import Definitions,ScheduleDefinition,define_asset_job, load_assets_from_modules ,AssetSelection 
from crowding_pipeline import assets, assets_2 
from .resources import Sf_ressource 



all_assets = load_assets_from_modules([assets])
agg_assets = load_assets_from_modules([assets_2])

# defining a job that targets all the assets 
tfl_pipeline_job = define_asset_job(
    name="tfl_pipeline_job",
    description="""the TFL job is Data pipeline tat loads the data from the TFL API and passed it to a series of cleaning processing and feature inginnering to finally return a clean reliable and 
    ready to use data """,
    selection=AssetSelection.groups("ingestion", "Loading_raw", "cleaning", "processing", "feature_engineering", "loading_clean_data")
)

# create a schedule for that job 
tfl_schudule=ScheduleDefinition(
    job=tfl_pipeline_job,
    cron_schedule="*/5 * * * *",
    name = "tfl_schedule"
)

#-- creating the second job for the aggregation pipeline 
agg_job = define_asset_job(
    name="tfl_agg_job",
    description="""
        this pipeline creats daily aggregation of the CLEAN_DATA it's triggers each day at 01:00 PM 
    """,
    selection=AssetSelection.groups("Aggregation")
)
agg_schedule = ScheduleDefinition(
    job=agg_job,
    cron_schedule="* 1 * * *",  # 2:30 PM daily
    name="tfl_schudule"
)

#-- general definition 
defs = Definitions(
    assets=[*all_assets,*agg_assets],
    jobs=[tfl_pipeline_job,agg_job],
    schedules=[tfl_schudule,agg_schedule],
    resources={
        "Sf_ressource":Sf_ressource
    }
)
