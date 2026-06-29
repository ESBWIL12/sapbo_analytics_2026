# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## Dedup Bronze WEBI Tables
# MAGIC
# MAGIC Deduplicates records from the 5 bronze tables produced by `SAP_BO_API_Extraction_Full`.  
# MAGIC Keeps only the latest `ingestion_ts` per natural key, stores results in DataFrames for downstream use.
# MAGIC
# MAGIC **Source tables:**
# MAGIC - `webi_parameters`
# MAGIC - `webi_variables`
# MAGIC - `webi_dataDictionary`
# MAGIC - `webi_metadata_cms`
# MAGIC - `webi_excel`

# COMMAND ----------

# DBTITLE 1,Dedup webi_parameters
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog_schema = "dataplatform01_central_dev_catalog_01.bronze_raw_sap_bo"

# webi_parameters: natural key = Document_Id + parameter_id
df_parameters = spark.table(f"{catalog_schema}.webi_parameters")

w = Window.partitionBy("Document_Id", "parameter_id").orderBy(F.col("ingestion_ts").desc())
df_parameters_dedup = df_parameters \
    .withColumn("_rank", F.row_number().over(w)) \
    .filter(F.col("_rank") == 1) \
    .drop("_rank")

print(f"webi_parameters: {df_parameters.count():,} raw → {df_parameters_dedup.count():,} deduped")

# COMMAND ----------

# DBTITLE 1,Dedup webi_variables
# webi_variables: natural key = Document_Id + variable_id
df_variables = spark.table(f"{catalog_schema}.webi_variables")

w = Window.partitionBy("Document_Id", "variable_id").orderBy(F.col("ingestion_ts").desc())
df_variables_dedup = df_variables \
    .withColumn("_rank", F.row_number().over(w)) \
    .filter(F.col("_rank") == 1) \
    .drop("_rank")

print(f"webi_variables: {df_variables.count():,} raw → {df_variables_dedup.count():,} deduped")

# COMMAND ----------

# DBTITLE 1,Dedup webi_dataDictionary
# webi_dataDictionary: natural key = Document_Id + Data_Provider_ID + datafield_id
df_datadict = spark.table(f"{catalog_schema}.webi_datadictionary")

w = Window.partitionBy("Document_Id", "Data_Provider_ID", "datafield_id").orderBy(F.col("ingestion_ts").desc())
df_datadict_dedup = df_datadict \
    .withColumn("_rank", F.row_number().over(w)) \
    .filter(F.col("_rank") == 1) \
    .drop("_rank")

print(f"webi_dataDictionary: {df_datadict.count():,} raw → {df_datadict_dedup.count():,} deduped")

# COMMAND ----------

# DBTITLE 1,Dedup webi_metadata_cms
# webi_metadata_cms: natural key = Document_Id + Data_Provider_ID + SQL_Index
df_metadata_cms = spark.table(f"{catalog_schema}.webi_metadata_cms")

w = Window.partitionBy("Document_Id", "Data_Provider_ID", "SQL_Index").orderBy(F.col("ingestion_ts").desc())
df_metadata_cms_dedup = df_metadata_cms \
    .withColumn("_rank", F.row_number().over(w)) \
    .filter(F.col("_rank") == 1) \
    .drop("_rank")

print(f"webi_metadata_cms: {df_metadata_cms.count():,} raw → {df_metadata_cms_dedup.count():,} deduped")

# COMMAND ----------

# DBTITLE 1,Dedup webi_excel
# webi_excel: natural key = Document_Id + Data_Provider_ID
df_excel = spark.table(f"{catalog_schema}.webi_excel")

w = Window.partitionBy("Document_Id", "Data_Provider_ID").orderBy(F.col("ingestion_ts").desc())
df_excel_dedup = df_excel \
    .withColumn("_rank", F.row_number().over(w)) \
    .filter(F.col("_rank") == 1) \
    .drop("_rank")

print(f"webi_excel: {df_excel.count():,} raw → {df_excel_dedup.count():,} deduped")

# COMMAND ----------

# DBTITLE 1,Summary
# Summary of all deduped DataFrames
print("\n=== DEDUP SUMMARY ===")
print(f"{'Table':<25} {'Raw':>10} {'Deduped':>10} {'Removed':>10}")
print("-" * 60)

for name, raw, dedup in [
    ("webi_parameters", df_parameters, df_parameters_dedup),
    ("webi_variables", df_variables, df_variables_dedup),
    ("webi_dataDictionary", df_datadict, df_datadict_dedup),
    ("webi_metadata_cms", df_metadata_cms, df_metadata_cms_dedup),
    ("webi_excel", df_excel, df_excel_dedup),
]:
    raw_count = raw.count()
    dedup_count = dedup.count()
    print(f"{name:<25} {raw_count:>10,} {dedup_count:>10,} {raw_count - dedup_count:>10,}")

print("\n✓ DataFrames available: df_parameters_dedup, df_variables_dedup, df_datadict_dedup, df_metadata_cms_dedup, df_excel_dedup")
