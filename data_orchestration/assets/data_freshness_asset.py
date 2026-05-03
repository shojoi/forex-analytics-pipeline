import os
from datetime import datetime
from dagster import asset, AssetExecutionContext, Output


@asset(
    name="data_freshness_tracker",
    description="Updates a Snowflake table to track when data was last refreshed",
    compute_kind="snowflake",
    group_name="monitoring",
    deps=["mart_daily_rates", "mart_currency_strength_index", "mart_monthly_snapshots"]
)
def data_freshness_tracker(context: AssetExecutionContext):
    """
    Creates/updates a table in Snowflake that tracks data freshness.
    Preset dashboards can query this table to show last update time.
    
    This table serves as the trigger indicator for Preset dashboards.
    Users can query this table in Preset to see when data was last updated.
    """
    
    from snowflake.connector import connect
    
    conn = connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USERNAME"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    
    cursor = conn.cursor()
    
    try:
        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS FOREX_DB.STAGING.DATA_FRESHNESS (
                pipeline_name VARCHAR(100),
                last_updated_at TIMESTAMP_NTZ,
                status VARCHAR(50),
                updated_by VARCHAR(100),
                run_id VARCHAR(200)
            )
        """)
        
        # Get run ID for tracking
        run_id = context.run.run_id if hasattr(context, 'run') else 'manual'
        
        # Update or insert freshness record
        cursor.execute(f"""
            MERGE INTO FOREX_DB.STAGING.DATA_FRESHNESS AS target
            USING (SELECT 
                'forex_analytics_pipeline' AS pipeline_name,
                CURRENT_TIMESTAMP() AS last_updated_at,
                'SUCCESS' AS status,
                'dagster' AS updated_by,
                '{run_id}' AS run_id
            ) AS source
            ON target.pipeline_name = source.pipeline_name
            WHEN MATCHED THEN 
                UPDATE SET 
                    last_updated_at = source.last_updated_at,
                    status = source.status,
                    updated_by = source.updated_by,
                    run_id = source.run_id
            WHEN NOT MATCHED THEN
                INSERT (pipeline_name, last_updated_at, status, updated_by, run_id)
                VALUES (source.pipeline_name, source.last_updated_at, source.status, 
                        source.updated_by, source.run_id)
        """)
        
        conn.commit()
        
        current_time = datetime.now().isoformat()
        context.log.info(f"✅ Data freshness tracker updated successfully at {current_time}")
        context.log.info(f"📊 Preset dashboards can now be refreshed with latest data")
        
        return Output(
            value={
                "last_updated": current_time,
                "status": "SUCCESS",
                "run_id": run_id
            },
            metadata={
                "last_updated": current_time,
                "records_updated": 1,
                "run_id": run_id,
                "message": "Data ready for Preset refresh"
            }
        )
    
    finally:
        cursor.close()
        conn.close()