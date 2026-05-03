from dagster import ScheduleDefinition, AssetSelection

# Daily schedule for complete pipeline
forex_daily_schedule = ScheduleDefinition(
    name="forex_daily_pipeline",
    target=AssetSelection.all(),
    cron_schedule="0 2 * * *",  # Daily at 2am (after manual Airbyte sync at ~1 AM)
    description="Runs complete forex pipeline daily at midnight"
)

"""
# Hourly schedule (optional - for frequent updates)
forex_hourly_schedule = ScheduleDefinition(
    name="forex_hourly_pipeline",
    target=AssetSelection.all(),
    cron_schedule="0 * * * *",  # Every hour
    description="Runs complete forex pipeline every hour"
)

# DBT-only schedule
forex_daily_dbt_schedule = ScheduleDefinition(
    name="daily_forex_transformation",
    target=AssetSelection.groups("default"),
    cron_schedule="0 1 * * *",  # Daily at 1 AM
    description="Runs only dbt transformations daily"
)
"""