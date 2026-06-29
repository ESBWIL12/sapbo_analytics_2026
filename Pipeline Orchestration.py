# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,SAP BO Analytics Pipeline
# MAGIC %md
# MAGIC ## SAP BO Analytics — Pipeline Orchestration
# MAGIC
# MAGIC Runs the full data pipeline from bronze ingestion through to final output tables.
# MAGIC
# MAGIC **Execution order:**
# MAGIC 0. CMS File Location Scan → enriches `webi_metadata_cms` (`File_location`, `File_format`)
# MAGIC 1. WEBI Lineage (Steps 1-3) → `webi_dictionary_linage`
# MAGIC 2. Variable Lead Lineage (Steps 1-6) → `webi_variables_linage`
# MAGIC 3. KPI Categorization (Cells 2-7) → categorized + `webi_data_entries`
# MAGIC 4. Verification → row counts for output tables
# MAGIC
# MAGIC > **Note:** Each downstream notebook handles its own dedup (keeps latest `ingestion_ts` per key).
# MAGIC > The standalone `Dedup Bronze Tables` notebook is available for ad-hoc exploration.
# MAGIC
# MAGIC **Bronze source tables:**
# MAGIC - `webi_parameters`, `webi_variables`, `webi_dataDictionary`, `webi_metadata_cms`, `webi_excel`
# MAGIC
# MAGIC **Final output tables:**
# MAGIC - `custom_sap_bo.webi_data_entries`
# MAGIC - `custom_sap_bo.pbi_webi_basic`
# MAGIC - `custom_sap_bo.pbi_webi_profile`

# COMMAND ----------

# DBTITLE 1,Config
from datetime import datetime

# Base paths
repo_root = "/Workspace/Users/baodi.wilkinson.external@atradius.com/SAP_BO_Analytics"

# Notebook paths
notebooks = {
    "cms_file_location": f"{repo_root}/New Extraction/Found File location",
    "webi": f"{repo_root}/PowerBI Prep/WEBI",
    "variable_lineage": f"{repo_root}/BO_Report_Classifications_KPI/Variable lead Lineage",
    "kpi": f"{repo_root}/PowerBI Prep/KPI_V2",
}

# Pipeline state tracking
pipeline_start = datetime.now()
pipeline_results = {}

def run_stage(stage_num, stage_name, notebook_key, timeout=1200):
    """Run a pipeline stage with error handling and timing."""
    print("=" * 60)
    print(f"STAGE {stage_num}: {stage_name}")
    print("=" * 60)
    start = datetime.now()
    try:
        result = dbutils.notebook.run(notebooks[notebook_key], timeout_seconds=timeout)
        elapsed = (datetime.now() - start).total_seconds()
        pipeline_results[stage_name] = {"status": "SUCCESS", "elapsed_s": elapsed, "result": result}
        print(f"\u2713 Stage {stage_num} complete ({elapsed:.0f}s)")
        return True
    except Exception as e:
        elapsed = (datetime.now() - start).total_seconds()
        pipeline_results[stage_name] = {"status": "FAILED", "elapsed_s": elapsed, "error": str(e)}
        print(f"\u2717 Stage {stage_num} FAILED after {elapsed:.0f}s: {e}")
        return False

print("Pipeline notebooks:")
for stage, path in notebooks.items():
    print(f"  {stage}: {path}")
print(f"\nPipeline started at: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# DBTITLE 1,Stage 0: CMS File Location Scan
stage0_ok = run_stage(0, "CMS File Location Scan", "cms_file_location", timeout=900)

if not stage0_ok:
    print("\n⚠ Stage 0 failed. File_location and File_format in webi_metadata_cms will not be updated.")
    print("  Continuing — downstream stages can proceed with existing file metadata.")

# COMMAND ----------

# DBTITLE 1,Stage 1: WEBI Dictionary Lineage (Steps 1-3)
stage1_ok = run_stage(1, "WEBI Dictionary Lineage", "webi", timeout=1200)

if not stage1_ok:
    print("\n⚠ Stage 1 failed. Stage 2 (Variable Lineage) depends on webi_datadictionary_temp.")
    print("  Skipping remaining stages.")
    dbutils.notebook.exit("FAILED at Stage 1: WEBI Dictionary Lineage")

# COMMAND ----------

# DBTITLE 1,Stage 2: Variable Lead Lineage
stage2_ok = run_stage(2, "Variable Lead Lineage", "variable_lineage", timeout=1800)

if not stage2_ok:
    print("\n⚠ Stage 2 failed. webi_variables_linage will not be updated.")
    print("  Stage 3 (KPI) will use stale variable data but can still categorize dictionary.")
    # Continue — KPI categorization can partially succeed with existing webi_variables_linage

# COMMAND ----------

# DBTITLE 1,Stage 3: KPI Categorization + webi_data_entries
stage3_ok = run_stage(3, "KPI Categorization", "kpi", timeout=1800)

if not stage3_ok:
    print("\n⚠ Stage 3 failed. webi_data_entries may not be updated.")
    print("  Continuing to verification to check what succeeded.")

# COMMAND ----------

# DBTITLE 1,Stage 4: Verify final output tables
from pyspark.sql import functions as F

print("=" * 60)
print("STAGE 4: Verification")
print("=" * 60)

# Check output tables
output_tables = [
    "dataplatform01_central_dev_catalog_01.custom_sap_bo.webi_data_entries",
    "dataplatform01_central_dev_catalog_01.custom_sap_bo.pbi_webi_basic",
    "dataplatform01_central_dev_catalog_01.custom_sap_bo.pbi_webi_profile",
]

print(f"\n{'Table':<60} {'Rows':>10} {'Columns':>10}")
print("-" * 85)
for t in output_tables:
    try:
        df = spark.table(t)
        print(f"{t:<60} {df.count():>10,} {len(df.columns):>10}")
    except Exception as e:
        print(f"{t:<60} {'ERROR':>10} {str(e)[:30]}")

# Pipeline summary
pipeline_elapsed = (datetime.now() - pipeline_start).total_seconds()
print(f"\n{'=' * 60}")
print(f"PIPELINE SUMMARY ({pipeline_elapsed:.0f}s total)")
print(f"{'=' * 60}")
print(f"\n{'Stage':<30} {'Status':<10} {'Time':>8}")
print("-" * 50)
for stage_name, info in pipeline_results.items():
    status = info['status']
    elapsed = f"{info['elapsed_s']:.0f}s"
    marker = "\u2713" if status == "SUCCESS" else "\u2717"
    print(f"{marker} {stage_name:<28} {status:<10} {elapsed:>8}")

failed = [s for s, i in pipeline_results.items() if i['status'] == 'FAILED']
if failed:
    print(f"\n\u2717 Pipeline completed with failures: {', '.join(failed)}")
    dbutils.notebook.exit(f"PARTIAL: {len(failed)} stage(s) failed")
else:
    print(f"\n\u2713 Pipeline complete — all stages succeeded")
    dbutils.notebook.exit("SUCCESS")

# COMMAND ----------


