import os
import requests
from dagster import sensor, RunRequest, SensorEvaluationContext, SkipReason
from datetime import datetime


# -------------------------------------------------
# ENV CONFIG for Self-Hosted Airbyte
# -------------------------------------------------
#AIRBYTE_REST_API_BASE_URL = os.getenv("AIRBYTE_REST_API_BASE_URL", "http://localhost:8006")

# ENV CONFIG for Airbyte Cloud
AIRBYTE_REST_API_BASE_URL = os.getenv("AIRBYTE_REST_API_BASE_URL", "https://api.airbyte.com")

AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID")
AIRBYTE_CLIENT_ID = os.getenv("AIRBYTE_CLIENT_ID")
AIRBYTE_CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET")


# -------------------------------------------------
# Helper: Get latest Airbyte job for a connection
# -------------------------------------------------
def get_latest_job():
    """Fetch the latest sync job from self-hosted Airbyte"""
    
    if not AIRBYTE_CONNECTION_ID:
        raise ValueError("AIRBYTE_CONNECTION_ID must be set")
    
    url = f"{AIRBYTE_REST_API_BASE_URL}/v1/jobs"

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {AIRBYTE_CLIENT_SECRET}"  # Cloud uses Bearer token
    }

    
    #headers = {
    #    "accept": "application/json",
    #}
    
    # For self-hosted Airbyte with abctl, use basic auth
    #auth = (AIRBYTE_CLIENT_ID, AIRBYTE_CLIENT_SECRET) if AIRBYTE_CLIENT_ID else None
    
    params = {
        "connectionId": AIRBYTE_CONNECTION_ID,
        "limit": 1,
        "orderBy": "createdAt|DESC"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        #response = requests.get(url, headers=headers, params=params, auth=auth, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        jobs = data.get("data", [])
        return jobs[0] if jobs else None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Airbyte jobs: {e}")
        return None


# -------------------------------------------------
# Airbyte Completion Sensor
# -------------------------------------------------
@sensor(
    name="airbyte_sync_sensor",
    minimum_interval_seconds=60,
    description="Monitors self-hosted Airbyte sync completion and triggers downstream dbt pipeline"
)
def airbyte_sync_sensor(context: SensorEvaluationContext):
    """
    Self-hosted Airbyte sensor:
    - Checks latest sync job status
    - Triggers downstream assets when sync succeeds
    - Prevents duplicate runs using cursor
    """
    
    try:
        job = get_latest_job()
        
        if not job:
            return SkipReason("No Airbyte jobs found for this connection.")
        
        job_id = job.get("jobId") or job.get("id")
        status = job.get("status")
        
        context.log.info(f"Found Airbyte job {job_id} with status: {status}")
        
        # Only proceed if sync completed successfully
        if status not in ["succeeded", "success", "completed"]:
            return SkipReason(f"Latest job not ready. Status: {status}")
        
        # Prevent duplicate runs using cursor
        if context.cursor == str(job_id):
            return SkipReason(f"Job {job_id} already processed.")
        
        # Update cursor BEFORE triggering
        context.update_cursor(str(job_id))
        
        context.log.info(f"✅ Airbyte sync {job_id} completed successfully. Triggering downstream assets.")
        
        return RunRequest(
            run_key=str(job_id),
            tags={
                "airbyte_job_id": str(job_id),
                "source": "airbyte_self_hosted",
                "triggered_at": datetime.now().isoformat(),
            },
        )
    
    except Exception as e:
        context.log.error(f"Sensor error: {str(e)}")
        return SkipReason(f"Sensor error: {str(e)}")