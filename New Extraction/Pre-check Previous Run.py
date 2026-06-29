# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Check if previous run succeeded
import requests
import json

# Get current job context (reliable method using notebook context tags)
ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
job_id = ctx.get("tags", {}).get("jobId")

if not job_id:
    print("Not running as part of a job — skipping pre-check.")
    dbutils.notebook.exit("SKIP_CHECK")

# Get workspace URL and token for API call
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {"Authorization": f"Bearer {token}"}

# List recent completed runs for this job (limit 2: current run + previous)
response = requests.get(
    f"https://{workspace_url}/api/2.1/jobs/runs/list",
    headers=headers,
    params={"job_id": job_id, "limit": 5, "completed_only": "true"}
)
response.raise_for_status()
runs = response.json().get("runs", [])

if len(runs) == 0:
    print("No previous completed runs found — proceeding (first run).")
    dbutils.notebook.exit("FIRST_RUN")

# The most recent completed run is the previous one
prev_run = runs[0]
prev_status = prev_run.get("state", {}).get("result_state", "UNKNOWN")
prev_run_id = prev_run.get("run_id")
prev_end = prev_run.get("end_time", "N/A")

print(f"Previous run ID: {prev_run_id}")
print(f"Previous result: {prev_status}")

if prev_status == "SUCCESS":
    print("\n✓ Previous run succeeded — proceeding with extraction.")
    dbutils.notebook.exit("SUCCESS")
else:
    msg = f"Previous run {prev_run_id} ended with status: {prev_status}. Skipping this run."
    print(f"\n✗ {msg}")
    raise Exception(msg)

# COMMAND ----------


