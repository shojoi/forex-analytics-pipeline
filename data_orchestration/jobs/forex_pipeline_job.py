from dagster import define_asset_job, AssetSelection

# Complete end-to-end pipeline job
forex_pipeline_job = define_asset_job(
    name="forex_complete_pipeline",
    description="Complete Forex pipeline: Airbyte → DBT → Data Freshness Tracker",
    selection=AssetSelection.all(),
    tags={
        "pipeline": "forex_analytics",
        "type": "end_to_end"
    }
)

# DBT-only job (for manual runs)
dbt_only_job = define_asset_job(
    name="dbt_transformation_only",
    description="Run only dbt transformations",
    selection=AssetSelection.groups("default"),
    tags={
        "pipeline": "dbt_only",
        "type": "transformation"
    }
)

# Airbyte-only job (for testing)
airbyte_only_job = define_asset_job(
    name="airbyte_sync_only",
    description="Run only Airbyte sync",
    selection=AssetSelection.groups("airbyte"),
    tags={
        "pipeline": "airbyte_only",
        "type": "extraction"
    }
)

# Data freshness update only
freshness_only_job = define_asset_job(
    name="update_data_freshness",
    description="Update data freshness tracker only",
    selection=AssetSelection.assets("data_freshness_tracker"),
    tags={
        "pipeline": "monitoring",
        "type": "freshness"
    }
)