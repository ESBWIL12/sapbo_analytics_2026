# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Arcade Risk Assessment Refresh Job
# MAGIC %md
# MAGIC ## Arcade Risk Assessment Refresh Job
# MAGIC Schedulable pipeline that refreshes the full Arcade Risk impact assessment.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Extract tables from SQL (incremental → `active_webi_source`)
# MAGIC 2. Build Table Linage:
# MAGIC    - 2a: `BOSQL_MI_Mapping_V3` (two-level dedup: SOURCE priority → SCHEMA priority)
# MAGIC    - 2b: `MI_Dictionary_SAP_BO` (recursive trace, ORADMART1 only, depth < 5)
# MAGIC    - 2c: Recreate `applications_databrickUC_Schema_mapping` (hardcoded lookup)
# MAGIC    - 2d: `Table_linage` (BO → MI → SYMP join)
# MAGIC 3. Data Entry SQL Linage (universe definitions → `active_webi_dataentry_linage_UCflagged`)
# MAGIC 4. Full Linage + Combined Output + Arcade Impact Assessment:
# MAGIC    - 4a: Build `active_webi_full_linage` (SQL-extracted)
# MAGIC    - 4b: Add BO_DataConnection (MERGE)
# MAGIC    - 4c: Build `active_webi_full_linage_UCflagged`
# MAGIC    - 4d: Combine both sources (dedup, prioritize Data entry)
# MAGIC    - 4e: Arcade Impact Assessment (project + release level)
# MAGIC    - 4f: Write `combined_linage_project_impact_report` + `combined_linage_release_impact_report`
# MAGIC
# MAGIC **Dependencies:** `sqlglot`

# COMMAND ----------

# DBTITLE 1,Step 0 - Install dependencies
# MAGIC %pip install sqlglot -q
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Step 1 - Extract tables from SQL (incremental)
import sqlglot
import re
from sqlglot.expressions import Table, CTE
from sqlglot.errors import ErrorLevel
from pyspark.sql.functions import udf, col, explode, split, when, size, element_at, trim, upper, row_number, desc
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window

def clean_bo_sql(sql):
    """Remove SAP BO-specific syntax that sqlglot cannot parse."""
    sql = re.sub(r"@Prompt\((?:[^()]*|\([^()]*\))*\)", "'PROMPT_PLACEHOLDER'", sql)
    sql = re.sub(r"@Variable\((?:[^()]*|\([^()]*\))*\)", "'VAR_PLACEHOLDER'", sql)
    sql = re.sub(r'\.\s+', '.', sql)
    return sql

def extract_tables(sql):
    try:
        clean_sql = clean_bo_sql(sql or "")
        tables = set()
        cte_names = set()
        for parsed in sqlglot.parse(clean_sql, read="oracle", error_level=ErrorLevel.IGNORE):
            for cte in parsed.find_all(CTE):
                cte_names.add(cte.alias.lower())
            for t in parsed.find_all(Table):
                catalog = t.catalog
                schema = t.db
                name = t.name
                full_name = ".".join(
                    part for part in [catalog, schema, name] if part
                )
                if full_name.lower() not in cte_names:
                    tables.add(full_name)
        return sorted(tables)
    except Exception as e:
        return []

extract_tables_udf = udf(extract_tables, ArrayType(StringType()))

# Step 1a: Get already-processed Document_Ids (Python set difference per user preference)
processed_ids = set(
    row.Document_Id for row in
    spark.sql("SELECT DISTINCT Document_Id FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_source").collect()
)
print(f"Already processed: {len(processed_ids)} documents")

# Step 1b: Get ALL candidate IDs (excluding error rows, no ingestion_ts filter)
candidate_ids = [row.Document_Id for row in
    spark.sql("""
        SELECT DISTINCT Document_Id 
        FROM dataplatform01_central_dev_catalog_01.bronze_raw_sap_bo.webi_metadata_cms
        WHERE SQL_Query NOT IN ('Error retrieving Query Plan', 'Data Source Type excel not handled for SQL extraction')
    """).collect()]
print(f"Total candidates: {len(candidate_ids)} documents")

# Step 1c: Find unprocessed IDs
document_ids = sorted(set(candidate_ids) - processed_ids)
print(f"New documents to process: {len(document_ids)}")

if document_ids:
    # Step 1d: Prepare source table with dedup (keep latest ingestion_ts per Document_Id/Provider/Index)
    df_source = spark.table("dataplatform01_central_dev_catalog_01.bronze_raw_sap_bo.webi_metadata_cms")
    w = Window.partitionBy("Document_Id", "Data_Provider_ID", "SQL_Index").orderBy(desc("ingestion_ts"))
    df_deduped = df_source.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")

    # Step 1e: Process in batches of 1000
    batch_size = 1000
    batches = [document_ids[i:i+batch_size] for i in range(0, len(document_ids), batch_size)]
    print(f"Processing {len(batches)} batches of up to {batch_size} documents each...")

    for idx, batch_ids in enumerate(batches):
        print(f"  Batch {idx+1}/{len(batches)}: {len(batch_ids)} documents...")
        df_filtered = df_deduped.filter(col("Document_Id").isin(batch_ids))

        # Apply UDF and explode tables
        df_with_tables = df_filtered.withColumn("parsed_tables", extract_tables_udf(col("SQL_Query")))
        df_exploded = df_with_tables.withColumn("table", explode(col("parsed_tables")))

        split_col = split(col("table"), r"\.")
        df_final = df_exploded.withColumn(
            "sql_table", upper(trim(col("table")))
        ).withColumn(
            "table_Name", upper(trim(element_at(split_col, -1)))
        ).withColumn(
            "schema_Name", when(size(split_col) > 1, upper(trim(element_at(split_col, -2))))
        )

        result = df_final.select(
            col("Document_Id"),
            col("Document_CUID"),
            col("Document_name"),
            col("Full_path"),
            col("lastAuthor").alias("updated_by"),
            col("Connection_Name").alias("source_DB_connection"),
            col("sql_table"),
            col("table_Name"),
            col("schema_Name")
        ).distinct()

        result.write.mode("append").saveAsTable(
            "dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_source"
        )
        print(f"  Batch {idx+1} written.")

    # Step 1f: Optimize table after all batches
    spark.sql("OPTIMIZE dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_source")
    print(f"Step 1 complete: processed {len(document_ids)} new documents, table optimized.")
else:
    print("Step 1 complete: no new documents to process")

# COMMAND ----------

# DBTITLE 1,Step 2 - Tables Linage (BO → MI → SYMP)
# Step 2: Rebuild Table Linage chain (BO SQL → MI → SYMP source)
# Equivalent to Stages 1-3 in the Tables linage notebook

print("Step 2a: Building BOSQL_MI_Mapping_V3...")
spark.sql("""
CREATE OR REPLACE TABLE dataplatform01_central_dev_catalog_01.bronze_raw_sap_bo.BOSQL_MI_Mapping_V3 AS
WITH raw_data AS (
  SELECT DISTINCT
    upper(trim(table)) AS BO_TABLE,
    CASE 
      WHEN upper(SOURCE) LIKE 'ALIAS%' AND base_table IS NOT NULL AND base_table != '' 
        THEN upper(trim(base_table)) 
      ELSE upper(trim(TABLE)) 
    END AS SOURCE_TABLE,
    upper(trim(SCHEMA)) AS SCHEMA,
    CASE
      WHEN upper(SOURCE) LIKE 'ALIAS%'
        THEN upper(trim(regexp_replace(regexp_replace(SOURCE, '(?i)^Alias\\s*', ''), '[()\\[\\]{}]', '')))
      ELSE upper(trim(SOURCE))
    END AS SOURCE
  FROM dataplatform01_modelling_dev_catalog_01.bo_universes_excel_sheet_imports.v3_extraction
  UNION
  SELECT DISTINCT
    upper(trim(table)) AS BO_TABLE,
    CASE 
      WHEN upper(SOURCE) LIKE 'ALIAS%' AND base_table IS NOT NULL AND base_table != '' 
        THEN upper(trim(base_table)) 
      ELSE upper(trim(TABLE)) 
    END AS SOURCE_TABLE,
    upper(trim(SCHEMA)) AS SCHEMA,
    CASE
      WHEN upper(SOURCE) LIKE 'ALIAS%'
        THEN upper(trim(regexp_replace(regexp_replace(SOURCE, '(?i)^Alias\\s*', ''), '[()\\[\\]{}]', '')))
      ELSE upper(trim(SOURCE))
    END AS SOURCE
  FROM dataplatform01_modelling_dev_catalog_01.bo_universes_excel_sheet_imports.v3_extraction_remaining
)
-- Level 1: Deduplicate by (BO_TABLE, SOURCE_TABLE, SCHEMA) — keep highest priority SOURCE
ranked_source AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY BO_TABLE, SOURCE_TABLE, SCHEMA
      ORDER BY
        CASE 
          WHEN SOURCE = 'SYMPHONY' THEN 1
          WHEN SOURCE = 'ORACLE FINANCE' THEN 2
          WHEN SOURCE = 'ORACLE FINANCE TABLE' THEN 3
          WHEN SOURCE = 'ORACLE FINANCE VIEW' THEN 4
          WHEN SOURCE = 'ORACLEFINANCE' THEN 5
          WHEN SOURCE = 'ORACLE FINANCE MATERIALIZED VIEW' THEN 6
          WHEN SOURCE = 'ORACLE FINACNE' THEN 7
          WHEN SOURCE = 'DW (FACT)' THEN 8
          WHEN SOURCE = 'DW' THEN 9
          WHEN SOURCE = 'DW (DIMENSION)' THEN 10
          WHEN SOURCE = 'DW (VIEW)' THEN 11
          WHEN SOURCE = 'INFORMATICA' THEN 12
          WHEN SOURCE = 'SYSTEM TABLE' THEN 13
          WHEN SOURCE = 'DERIVED' THEN 14
          WHEN SOURCE IN ('NA', 'NOT AVAILABLE') THEN 15
          ELSE 99
        END
    ) AS rn1
  FROM raw_data
  WHERE BO_TABLE IS NOT NULL AND BO_TABLE != ''
),
deduped_source AS (
  SELECT BO_TABLE, SOURCE_TABLE, SCHEMA, SOURCE
  FROM ranked_source
  WHERE rn1 = 1
),
-- Level 2: Deduplicate by (BO_TABLE, SOURCE_TABLE) — keep highest priority SCHEMA
ranked_schema AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY BO_TABLE, SOURCE_TABLE
      ORDER BY
        CASE 
          WHEN SCHEMA = 'ORADMART1' THEN 1
          WHEN SCHEMA = 'ORABUP0' THEN 2
          WHEN SCHEMA = 'AR' THEN 3
          WHEN SCHEMA = 'GL' THEN 4
          WHEN SCHEMA = 'APPS' THEN 5
          WHEN SCHEMA = 'AP' THEN 6
          WHEN SCHEMA = 'CUST' THEN 7
          WHEN SCHEMA = 'FA' THEN 8
          WHEN SCHEMA = 'APPLSYS' THEN 9
          WHEN SCHEMA = 'PO' THEN 10
          WHEN SCHEMA = 'HR' THEN 11
          WHEN SCHEMA = 'XLA' THEN 12
          WHEN SCHEMA = 'JTF' THEN 13
          WHEN SCHEMA = 'INV' THEN 14
          WHEN SCHEMA = 'ZX' THEN 15
          WHEN SCHEMA = 'OKC' THEN 16
          WHEN SCHEMA = 'ICX' THEN 17
          WHEN SCHEMA = 'SYS' THEN 18
          WHEN SCHEMA = 'ORABOFP' THEN 19
          WHEN SCHEMA = 'GBRELS1' THEN 20
          WHEN SCHEMA IN ('NOT AVAILABLE', 'NOT AVILABLE') THEN 21
          ELSE 99
        END
    ) AS rn2
  FROM deduped_source
)
SELECT BO_TABLE, SOURCE_TABLE, SCHEMA, SOURCE
FROM ranked_schema
WHERE rn2 = 1
""")
print("  BOSQL_MI_Mapping_V3 created.")

print("Step 2b: Building MI_Dictionary_SAP_BO (recursive trace)...")
spark.sql("""
CREATE OR REPLACE TABLE dataplatform01_central_dev_catalog_01.bronze_raw_sap_bo.MI_Dictionary_SAP_BO AS 
WITH RECURSIVE source_dict AS (
  SELECT DISTINCT
    upper(trim(TARGET_SCHEMA)) as TARGET_SCHEMA,
    upper(trim(TARGET_TABLE)) as TARGET_TABLE,
    upper(trim(SOURCE_SCHEMA)) as SOURCE_SCHEMA,
    upper(trim(SOURCE_TABLE)) as SOURCE_TABLE
  FROM dataplatform01_modelling_dev_catalog_01.bo_universes_excel_sheet_imports.mi_dwh_data_dictionary
  WHERE SOURCE_SCHEMA IS NOT NULL 
    AND upper(trim(SOURCE_SCHEMA)) NOT IN ('CREATED AND DROPPED', 'SYS')
    AND TARGET_TABLE IS NOT NULL
    AND upper(trim(TARGET_SCHEMA)) NOT IN ('CREATED AND DROPPED')
),
lineage AS (
  SELECT 
    TARGET_SCHEMA AS origin_target_schema,
    TARGET_TABLE AS origin_target_table,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    1 AS depth
  FROM source_dict
  UNION ALL
  SELECT 
    l.origin_target_schema,
    l.origin_target_table,
    d.SOURCE_SCHEMA,
    d.SOURCE_TABLE,
    l.depth + 1
  FROM lineage l
  INNER JOIN source_dict d
    ON l.SOURCE_SCHEMA = d.TARGET_SCHEMA AND l.SOURCE_TABLE = d.TARGET_TABLE
  WHERE l.SOURCE_SCHEMA = 'ORADMART1'
    AND l.depth < 5
)
SELECT DISTINCT
  origin_target_schema AS TARGET_SCHEMA,
  origin_target_table AS TARGET_TABLE,
  SOURCE_SCHEMA,
  SOURCE_TABLE
FROM lineage
WHERE SOURCE_SCHEMA != 'ORADMART1'
""")
print("  MI_Dictionary_SAP_BO created.")

print("Step 2c: Recreating applications_databrickUC_Schema_mapping...")
spark.sql("""
create or replace table dataplatform01_central_dev_catalog_01.custom_sap_bo.applications_databrickUC_Schema_mapping AS
SELECT 'ORACLE FINANCE TABLE' AS BO_SOURCE, 'AP' AS BO_source_schema, 'bronze_raw_orf_ap' AS Databricks_Schema
UNION ALL SELECT 'ORACLE FINANCE TABLE', 'APPLSYS', 'bronze_raw_orf_applsys'
UNION ALL SELECT 'ORACLE FINANCE TABLE', 'APPS', 'bronze_raw_orf_apps'
UNION ALL SELECT 'ORACLE FINANCE TABLE', 'AR', 'bronze_raw_orf_ar'
UNION ALL SELECT 'ORACLE FINANCE TABLE', 'CUST', 'bronze_raw_orf_cust'
UNION ALL SELECT 'ORACLE FINANCE TABLE', 'FA', 'bronze_raw_orf_fa'
UNION ALL SELECT 'ORACLE FINANCE TABLE', 'GL', 'bronze_raw_orf_gl'
UNION ALL SELECT 'ORACLE FINANCE TABLE', 'HR', 'bronze_raw_orf_hr'
UNION ALL SELECT 'ORACLE FINANCE TABLE', 'INV', 'bronze_raw_orf_inv'
UNION ALL SELECT 'ORACLE FINANCE TABLE', 'JTF', 'bronze_raw_orf_jtf'
UNION ALL SELECT 'ORACLE FINANCE TABLE', 'ORABOFP', 'bronze_raw_orf_orabofp'
UNION ALL SELECT 'SYMPHONY', 'ORABUP0', 'bronze_raw_sym_orabup0'
UNION ALL SELECT 'DW', 'ORABUP0', 'bronze_raw_symq_orabup0'
UNION ALL SELECT 'DW', 'ORADMART1', 'bronze_raw_symq_oradmart1'
UNION ALL SELECT 'DW', 'ORFM', NULL
UNION ALL SELECT 'DW', 'ORFP', NULL
UNION ALL SELECT 'ORACLE FINANCE TABLE', 'PO', 'bronze_raw_orf_po'
""")
print("  applications_databrickUC_Schema_mapping created.")

print("Step 2d: Building Table_linage...")
spark.sql("""
create or replace table dataplatform01_central_dev_catalog_01.custom_sap_bo.Table_linage as
select 
  upper(trim(v3.BO_TABLE)) as BO_SQL_TABLE, 
  upper(trim(v3.SOURCE_TABLE)) as MI_Table, 
  upper(trim(v3.SCHEMA)) as MI_SCHEMA, 
  upper(trim(v3.SOURCE)) as MI_SOURCE, 
  upper(trim(d1.SOURCE_TABLE)) as Src_table, 
  upper(trim(d1.SOURCE_SCHEMA)) as Src_schema 
from dataplatform01_central_dev_catalog_01.bronze_raw_sap_bo.BOSQL_MI_Mapping_V3 as V3
left join dataplatform01_central_dev_catalog_01.bronze_raw_sap_bo.MI_Dictionary_SAP_BO as D1
on upper(trim(v3.SOURCE_TABLE)) = upper(trim(D1.TARGET_TABLE)) 
   and upper(trim(v3.SCHEMA)) = upper(trim(D1.TARGET_SCHEMA))
""")
print("  Table_linage created.")
print("Step 2 complete.")

# COMMAND ----------

# DBTITLE 1,Step 3 - Data Entry SQL Linage
from pyspark.sql.functions import col, upper, trim, split, explode, size, element_at, when, lit, coalesce, regexp_extract_all, concat_ws, array_sort, collect_set, current_timestamp
from pyspark.sql.functions import row_number, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import split as spark_split

# Step 3: Data Entry SQL Linage (from webi_data_entries universe definitions)
print("Step 3a: Extracting BO_TABLE and BO_SCHEMA from sql_definition...")

df_entries = spark.table("dataplatform01_central_dev_catalog_01.custom_sap_bo.webi_data_entries")

# Extract all dotted identifier chains (TABLE.COLUMN or SCHEMA.TABLE.COLUMN)
pattern = r"([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+)"

df_extracted = (
    df_entries
    .filter(col("sql_definition").isNotNull() & (col("sql_definition") != "") & col("sql_definition").contains("."))
    .withColumn("dotted_chain", explode(regexp_extract_all(col("sql_definition"), lit(pattern))))
)

split_chain = split(col("dotted_chain"), r"\.")

df_step1 = (
    df_extracted
    .withColumn("_parts", split_chain)
    .withColumn("_size", size(col("_parts")))
    .filter(col("_size") >= 2)
    .withColumn(
        "BO_TABLE",
        upper(trim(
            when(col("_size") == 2, element_at(col("_parts"), 1))
            .when(col("_size") >= 3, element_at(col("_parts"), 2))
        ))
    )
    .withColumn(
        "BO_SCHEMA",
        upper(trim(
            when(col("_size") >= 3, element_at(col("_parts"), 1))
        ))
    )
    .select("Document_Id", "Document_name", "BO_TABLE", "BO_SCHEMA")
    .distinct()
)
print(f"  Extracted {df_step1.count()} rows, {df_step1.select('Document_Id').distinct().count()} unique documents")

# Step 3b: Join cluster + Table_linage (deduped: one source per BO_SQL_TABLE)
print("Step 3b: Joining cluster and Table_linage...")

df_clusters = spark.table("dataplatform01_central_dev_catalog_01.custom_sap_bo.webi_cluster_details")
df_tl = spark.table("dataplatform01_central_dev_catalog_01.custom_sap_bo.Table_linage")

df_with_cluster = (
    df_step1
    .join(
        df_clusters.select(upper(trim(col("Document_Id"))).alias("_cluster_doc_id"), col("cluster")),
        upper(trim(df_step1["Document_Id"])) == col("_cluster_doc_id"), "left"
    ).drop("_cluster_doc_id")
)

# Dedup Table_linage: one source per BO_SQL_TABLE (prefer non-null Src_table)
w_tl = Window.partitionBy("_tl_bo_table").orderBy(
    when(col("Src_table").isNotNull(), 0).otherwise(1),
    col("Src_table").asc_nulls_last(),
    col("MI_Table").asc_nulls_last()
)
df_tl_dedup = (
    df_tl.select(
        upper(trim(col("BO_SQL_TABLE"))).alias("_tl_bo_table"),
        upper(trim(col("MI_Table"))).alias("MI_Table"),
        upper(trim(col("MI_SCHEMA"))).alias("MI_SCHEMA"),
        upper(trim(col("MI_SOURCE"))).alias("MI_SOURCE"),
        upper(trim(col("Src_table"))).alias("Src_table"),
        upper(trim(col("Src_schema"))).alias("Src_schema"),
    )
    .withColumn("_rn", row_number().over(w_tl))
    .filter(col("_rn") == 1).drop("_rn")
)

df_step2 = (
    df_with_cluster
    .join(df_tl_dedup, upper(trim(df_with_cluster["BO_TABLE"])) == col("_tl_bo_table"), "left")
    .drop("_tl_bo_table")
    .withColumn("Calc_source_table", coalesce(col("Src_table"), col("MI_Table"), col("BO_TABLE")))
    .withColumn("Calc_source_schema",
        when(col("Src_table").isNull() & col("MI_Table").isNull(), col("BO_SCHEMA"))
        .when(col("Src_table").isNull(), col("MI_SCHEMA"))
        .otherwise(col("Src_schema"))
    )
    .select("Document_Id", "Document_name", "cluster", "BO_TABLE", "BO_SCHEMA",
            "MI_Table", "MI_SCHEMA", "MI_SOURCE", "Src_table", "Src_schema",
            "Calc_source_table", "Calc_source_schema")
    .distinct()
)

# Step 3c: Add BO_DataConnection
print("Step 3c: Adding BO_DataConnection...")
df_cms = spark.table("dataplatform01_central_dev_catalog_01.bronze_raw_sap_bo.webi_metadata_cms")
df_connections = (
    df_cms.groupBy(upper(trim(col("Document_Id"))).alias("_conn_doc_id"))
    .agg(concat_ws("|", array_sort(collect_set(upper(trim(col("Connection_Name")))))).alias("BO_DataConnection"))
)
df_step3 = (
    df_step2.join(df_connections, upper(trim(df_step2["Document_Id"])) == col("_conn_doc_id"), "left")
    .drop("_conn_doc_id")
)

# Step 3d: UC Schema mapping + ingestion flag
print("Step 3d: UC Schema mapping and ingestion flag...")
df_db_schema = spark.table("dataplatform01_central_dev_catalog_01.custom_sap_bo.applications_databrickUC_Schema_mapping")
df_prd_tables = (
    spark.table("dataplatform01_central_prd_catalog_01.information_schema.tables")
    .select(upper(trim(col("table_schema"))).alias("_prd_schema"), upper(trim(col("table_name"))).alias("_prd_table"))
    .distinct()
)

df_with_schema = (
    df_step3.join(
        df_db_schema.select(
            upper(trim(col("BO_SOURCE"))).alias("_db_bo_source"),
            upper(trim(col("BO_source_schema"))).alias("_db_bo_schema"),
            col("Databricks_Schema")
        ),
        (upper(trim(df_step3["Calc_source_schema"])) == col("_db_bo_schema"))
        & (
            (upper(trim(df_step3["Calc_source_schema"])) != "ORABUP0")
            | ((upper(trim(df_step3["Calc_source_schema"])) == "ORABUP0")
               & (upper(trim(spark_split(df_step3["MI_SOURCE"], " ")[0])) == col("_db_bo_source")))
        ),
        "left"
    ).drop("_db_bo_source", "_db_bo_schema")
)

df_dataentry_final = (
    df_with_schema.join(
        df_prd_tables,
        (upper(trim(col("Databricks_Schema"))) == col("_prd_schema"))
        & (upper(trim(df_with_schema["Calc_source_table"])) == col("_prd_table")),
        "left"
    )
    .withColumn("databricks_ingested", when(col("_prd_schema").isNull() | col("_prd_table").isNull(), "N").otherwise("Y"))
    .drop("_prd_schema", "_prd_table")
    .distinct()
)

# Write output
df_dataentry_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_dataentry_linage_UCflagged"
)
print(f"Step 3 complete: {df_dataentry_final.count()} rows written to active_webi_dataentry_linage_UCflagged")

# COMMAND ----------

# DBTITLE 1,Step 4 - Full Linage, Combined Output and Arcade Impact
from pyspark.sql.functions import col, upper, trim, coalesce, when, lit, row_number, desc, current_timestamp, concat_ws, array_sort, collect_set
from pyspark.sql.functions import split as spark_split
from pyspark.sql.window import Window

# ============================================================
# Step 4a: Build active_webi_full_linage (from SQL-extracted source)
# ============================================================
print("Step 4a: Building active_webi_full_linage...")

spark.sql("""
create or replace table dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_full_linage as
SELECT distinct
  upper(trim(aws.Document_Id)) as Document_Id,
  upper(trim(aws.Document_name)) as Document_name,
  wcd.cluster,
  upper(trim(aws.source_DB_connection)) as SAP_BO_Connection,
  upper(trim(aws.table_Name)) as BO_TABLE,
  upper(trim(aws.schema_Name)) as BO_SCHEMA,
  upper(trim(tl.MI_Table)) as MI_Table,
  upper(trim(tl.MI_SCHEMA)) as MI_SCHEMA,
  upper(trim(tl.MI_SOURCE)) as MI_SOURCE,
  upper(trim(tl.Src_table)) as Src_table,
  upper(trim(tl.Src_schema)) as Src_schema,
  COALESCE(upper(trim(tl.Src_table)), upper(trim(tl.MI_Table)), upper(trim(aws.table_Name))) as Calc_source_table,
  (CASE 
    WHEN upper(trim(tl.Src_table)) IS NULL AND upper(trim(tl.MI_Table)) IS NULL THEN upper(trim(aws.schema_Name))
    WHEN upper(trim(tl.Src_table)) IS NULL THEN upper(trim(tl.MI_SCHEMA))
    ELSE upper(trim(tl.Src_schema))
  END) as Calc_source_schema,
  upper(trim(aws.Document_CUID)) as Document_CUID,
  upper(trim(aws.Full_path)) as Full_path,
  upper(trim(aws.sql_table)) as sql_table,
  upper(trim(aws.updated_by)) as updated_by,
  current_timestamp() AS linage_ingestion_ts
FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_source AS aws
left join dataplatform01_central_dev_catalog_01.custom_sap_bo.webi_cluster_details as wcd
  ON upper(trim(aws.Document_Id)) = upper(trim(wcd.Document_Id))
LEFT JOIN dataplatform01_central_dev_catalog_01.custom_sap_bo.Table_linage AS tl
  ON upper(trim(aws.table_Name)) = upper(trim(tl.BO_SQL_TABLE))
""")
print("  active_webi_full_linage created.")

# Step 4b: Add BO_DataConnection via MERGE
print("Step 4b: Adding BO_DataConnection...")
spark.sql("""
ALTER TABLE dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_full_linage ADD COLUMN IF NOT EXISTS BO_DataConnection STRING
""")
spark.sql("""
MERGE INTO dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_full_linage AS awfl
USING (
  SELECT
    upper(trim(Document_Id)) AS Document_Id,
    concat_ws('|', array_sort(collect_set(upper(trim(Connection_Name))))) AS BO_DataConnection
  FROM dataplatform01_central_dev_catalog_01.bronze_raw_sap_bo.webi_metadata_cms
  GROUP BY upper(trim(Document_Id))
) AS conn
ON upper(trim(awfl.Document_Id)) = conn.Document_Id
WHEN MATCHED THEN UPDATE SET awfl.BO_DataConnection = conn.BO_DataConnection
""")
print("  BO_DataConnection merged.")

# Step 4c: Build UC-flagged version
print("Step 4c: Building active_webi_full_linage_UCflagged...")
spark.sql("""
create or replace table dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_full_linage_UCflagged as 
SELECT distinct
  fl1.*, DB_schema.Databricks_Schema, 
  (case when s1.table_schema is null or s1.table_name is null then 'N' else 'Y' end) as databricks_ingested
from dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_full_linage as fl1
LEFT JOIN dataplatform01_central_dev_catalog_01.custom_sap_bo.applications_databrickUC_Schema_mapping as DB_schema
  ON (
    upper(trim(fl1.Calc_source_schema)) = upper(trim(DB_schema.BO_source_schema))
    AND (
      upper(trim(fl1.Calc_source_schema)) <> 'ORABUP0'
      OR (
        upper(trim(fl1.Calc_source_schema)) = 'ORABUP0'
        AND upper(trim(split(fl1.MI_SOURCE, ' ')[0])) = upper(trim(DB_schema.BO_SOURCE))
      )
    )
  )
left join (SELECT distinct table_schema, table_name FROM dataplatform01_central_prd_catalog_01.information_schema.tables) as s1
  on upper(trim(DB_schema.Databricks_Schema))=upper(trim(s1.table_schema)) 
  and upper(trim(fl1.Calc_source_table))=upper(trim(s1.table_name))
""")
print("  active_webi_full_linage_UCflagged created.")

# ============================================================
# Step 4d: Combine both linage tables into final output
# ============================================================
print("Step 4d: Building combined linage output...")

df_dataentry = spark.table("dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_dataentry_linage_UCflagged")
df_sql_extracted = spark.table("dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_full_linage_UCflagged")

common_cols = [
    "Document_Id", "Document_name", "cluster",
    "BO_TABLE", "BO_SCHEMA",
    "Calc_source_table", "Calc_source_schema",
    "BO_DataConnection", "Databricks_Schema", "databricks_ingested"
]

df_de = (
    df_dataentry.select(*common_cols)
    .withColumn("SAP_BO_Connection", lit(None).cast("string"))
    .withColumn("source_type", lit("Data entry definition"))
)
df_sql = (
    df_sql_extracted.select(*common_cols, "SAP_BO_Connection")
    .withColumn("source_type", lit("SQL extracted"))
)

final_cols = [
    "Document_Id", "Document_name", "cluster", "SAP_BO_Connection",
    "BO_TABLE", "BO_SCHEMA",
    "Calc_source_table", "Calc_source_schema",
    "BO_DataConnection", "Databricks_Schema", "databricks_ingested", "source_type"
]

df_combined = df_de.select(*final_cols).unionByName(df_sql.select(*final_cols))

# Dedup: prioritize 'Data entry definition'
w_dedup = Window.partitionBy("Document_Id", "BO_TABLE", "Calc_source_table").orderBy(
    when(col("source_type") == "Data entry definition", 0).otherwise(1)
)
df_final = (
    df_combined
    .withColumn("_rn", row_number().over(w_dedup))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .withColumnRenamed("Calc_source_table", "Final_table")
    .withColumnRenamed("Calc_source_schema", "Final_schema")
    .distinct()
)
print(f"  Combined output: {df_final.count()} rows, {df_final.select('Document_Id').distinct().count()} unique documents")

# ============================================================
# Step 4e: Arcade Impact Assessment
# ============================================================
print("Step 4e: Arcade Impact Assessment...")

df_release_scope = spark.table("dataplatform01_central_dev_catalog_01.custom_sap_bo.arcaderisk_release_scope")
df_release_list = spark.table("dataplatform01_central_dev_catalog_01.custom_sap_bo.arcaderisk_release_list")

# --- Project impact per report ---
base = (
    df_final
    .join(
        df_release_scope.select(
            upper(trim(col("mi_table"))).alias("_scope_table"),
            col("Impacted_Flag"), col("Release_scheduled")
        ),
        col("Final_table") == col("_scope_table"), "left"
    ).drop("_scope_table")
    .withColumn("Prioritized",
        when((upper(trim(col("Impacted_Flag"))) == "Y") & (upper(trim(col("Release_scheduled"))) == "Y"), "Y")
        .when((upper(trim(col("Impacted_Flag"))) == "Y") & (upper(trim(col("Release_scheduled"))) == "N"), "N")
        .otherwise(None)
    )
    .withColumn("Impacted",
        when((upper(trim(col("Impacted_Flag"))) == "N") | col("Impacted_Flag").isNull(), "N").otherwise("Y")
    )
)

w_impact = Window.partitionBy("Document_Id").orderBy(
    desc("Impacted"),
    when(col("Prioritized") == "N", 1).when(col("Prioritized") == "Y", 2).otherwise(3)
)
df_impact_report = (
    base.withColumn("_rn", row_number().over(w_impact))
    .filter(col("_rn") == 1)
    .drop("_rn", "Impacted_Flag", "Release_scheduled")
    .withColumn("ingestion_ts", current_timestamp())
)

# --- Release-level impact per report ---
base_release = (
    df_final
    .join(
        df_release_list.select(
            upper(trim(col("mi_table"))).alias("_rel_table"),
            col("Impacted_Flag").alias("_rel_impact"), col("Release_schedule")
        ),
        col("Final_table") == col("_rel_table"), "left"
    ).drop("_rel_table")
    .withColumn("Prioritized",
        when((upper(trim(col("_rel_impact"))) == "Y") & col("Release_schedule").isNotNull(), "Y")
        .when((upper(trim(col("_rel_impact"))) == "Y") & col("Release_schedule").isNull(), "N")
        .otherwise(None)
    )
    .withColumn("Impacted",
        when((upper(trim(col("_rel_impact"))) == "N") | col("_rel_impact").isNull(), "N").otherwise("Y")
    ).drop("_rel_impact")
)

w_release = Window.partitionBy("Document_Id").orderBy(
    desc("Impacted"),
    when(col("Prioritized") == "N", 1).when(col("Prioritized") == "Y", 2).otherwise(3),
    desc("Release_schedule")
)
df_release_impact = (
    base_release.withColumn("_rn", row_number().over(w_release))
    .filter(col("_rn") == 1).drop("_rn")
    .withColumn("ingestion_ts", current_timestamp())
)

# ============================================================
# Step 4f: Write all outputs
# ============================================================
print("Step 4f: Writing output tables...")

df_impact_report.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "dataplatform01_central_dev_catalog_01.custom_sap_bo.combined_linage_project_impact_report"
)
print("  Written: combined_linage_project_impact_report")

df_release_impact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "dataplatform01_central_dev_catalog_01.custom_sap_bo.combined_linage_release_impact_report"
)
print("  Written: combined_linage_release_impact_report")

# Summary
print("\n" + "="*60)
print("ARCADE RISK ASSESSMENT REFRESH COMPLETE")
print("="*60)
print(f"  Project impact: {df_impact_report.count()} reports assessed")
print(f"  Release impact: {df_release_impact.count()} reports assessed")
df_impact_report.groupBy("Impacted", "Prioritized").count().orderBy("Impacted", "Prioritized").show()
df_release_impact.groupBy("Impacted", "Release_schedule").count().orderBy("Impacted", "Release_schedule").show()
