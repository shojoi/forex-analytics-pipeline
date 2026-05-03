from dagster import Definitions
from data_orchestration.assets.dbt_assets import forex_dbt_assets, dbt_warehouse_resource
#from data_orchestration.assets.airbyte_assets import airbyte_assets
from data_orchestration.assets.data_freshness_asset import data_freshness_tracker

from data_orchestration.jobs.forex_pipeline_job import (
    #forex_pipeline_job,
    dbt_only_job,
    #airbyte_only_job,
    freshness_only_job
)

from data_orchestration.schedules.schedules import (
    #forex_daily_schedule,
    #forex_hourly_schedule,
    forex_daily_dbt_schedule
)

#from data_orchestration.sensors.airbyte_sensor import airbyte_sync_sensor


# -----------------------------
# Resource Definitions
# -----------------------------
resources = {
    "dbt_warehouse_resource": dbt_warehouse_resource,
}


# -----------------------------
# Dagster Definitions (MAIN ENTRY)
# -----------------------------
defs = Definitions(
    assets=[
        # Airbyte extracts data from Currency Beacon API
        #airbyte_assets,
        
        # DBT transforms raw data into dimensional model
        forex_dbt_assets,
        
        # Data freshness tracker for Preset
        data_freshness_tracker,
    ],
    jobs=[
        #forex_pipeline_job,      # Complete pipeline
        dbt_only_job,            # DBT only
        #airbyte_only_job,        # Airbyte only
        freshness_only_job,      # Freshness only
    ],
    schedules=[
        #forex_daily_schedule,    # Daily at midnight (all assets)
        #forex_hourly_schedule,   # Hourly (all assets)
        forex_daily_dbt_schedule,# Daily at 1 AM (dbt only)
    ],
    #sensors=[ airbyte_sync_sensor,    ],     # Triggers on Airbyte completion
    resources=resources,
)