# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Comparison Overview
# MAGIC %md
# MAGIC ## Can `webi_data_entries` replace `active_webi_source`?
# MAGIC
# MAGIC **Conclusion: NO — `webi_data_entries` cannot replace `active_webi_source` for the lineage pipeline.**
# MAGIC
# MAGIC ### Key Findings
# MAGIC
# MAGIC | Metric | `active_webi_source` | `webi_data_entries` |
# MAGIC |---|---|---|
# MAGIC | Total rows | 201,403 | 128,986 |
# MAGIC | Unique documents | **21,829** | **4,537** |
# MAGIC | Unique tables | 916 | 1,047 (from sql_definition) |
# MAGIC | Documents in common | 3,972 | 3,972 |
# MAGIC | Documents NOT in the other | — | 17,857 docs MISSING |
# MAGIC | Table overlap | 267 shared | 649 only in aws / 780 only in wde |
# MAGIC
# MAGIC ### Why it can't replace:
# MAGIC 1. **82% document coverage gap** — only 4,537 of 21,829 reports are in `webi_data_entries`
# MAGIC 2. **Only 29% table overlap** — 649 tables in `active_webi_source` have no match in `webi_data_entries`
# MAGIC 3. **Different granularity** — `active_webi_source` has direct SQL table references; `webi_data_entries` has universe field-to-table mappings via `sql_definition`
# MAGIC 4. **Missing key columns** — no `schema_Name`, `source_DB_connection`, `Full_path` equivalents in `webi_data_entries`
# MAGIC
# MAGIC ### But it COULD complement:
# MAGIC - 780 additional tables from universe definitions not captured by SQL parsing
# MAGIC - 565 documents not in `active_webi_source`
# MAGIC - Richer metadata: Universe_Name, DataSource_Name, UNIVERSE_PRIMARY_CATEGORY

# COMMAND ----------

# DBTITLE 1,Coverage comparison stats
# MAGIC %sql
# MAGIC -- Coverage comparison: active_webi_source vs webi_data_entries
# MAGIC WITH aws_stats AS (
# MAGIC   SELECT 
# MAGIC     count(*) as total_rows,
# MAGIC     count(distinct Document_Id) as unique_docs,
# MAGIC     count(distinct table_Name) as unique_tables
# MAGIC   FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_source
# MAGIC ),
# MAGIC wde_stats AS (
# MAGIC   SELECT 
# MAGIC     count(*) as total_rows,
# MAGIC     count(distinct Document_Id) as unique_docs,
# MAGIC     count(distinct upper(trim(split(sql_definition, '\\.')[0]))) as unique_tables_from_sql_def
# MAGIC   FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.webi_data_entries
# MAGIC   WHERE sql_definition IS NOT NULL AND sql_definition != ''
# MAGIC ),
# MAGIC overlap AS (
# MAGIC   SELECT count(distinct a.Document_Id) as docs_in_both
# MAGIC   FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_source a
# MAGIC   INNER JOIN (SELECT DISTINCT Document_Id FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.webi_data_entries) b
# MAGIC     ON a.Document_Id = b.Document_Id
# MAGIC )
# MAGIC SELECT 
# MAGIC   'active_webi_source' as source, aws.total_rows, aws.unique_docs, aws.unique_tables as unique_tables, o.docs_in_both
# MAGIC FROM aws_stats aws, overlap o
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'webi_data_entries' as source, wde.total_rows, wde.unique_docs, wde.unique_tables_from_sql_def, o.docs_in_both
# MAGIC FROM wde_stats wde, overlap o

# COMMAND ----------

# DBTITLE 1,Table name overlap analysis
# MAGIC %sql
# MAGIC -- Table name overlap: how many tables are shared vs exclusive
# MAGIC WITH aws_tables AS (
# MAGIC   SELECT DISTINCT upper(trim(table_Name)) as tbl FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_source
# MAGIC   WHERE table_Name IS NOT NULL
# MAGIC ),
# MAGIC wde_tables AS (
# MAGIC   SELECT DISTINCT upper(trim(split(sql_definition, '\\.')[0])) as tbl 
# MAGIC   FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.webi_data_entries
# MAGIC   WHERE sql_definition IS NOT NULL AND sql_definition LIKE '%.%'
# MAGIC )
# MAGIC SELECT 
# MAGIC   (SELECT count(*) FROM aws_tables) as tables_in_active_webi_source,
# MAGIC   (SELECT count(*) FROM wde_tables) as tables_in_webi_data_entries,
# MAGIC   (SELECT count(*) FROM aws_tables a INNER JOIN wde_tables w ON a.tbl = w.tbl) as tables_in_both,
# MAGIC   (SELECT count(*) FROM aws_tables a LEFT JOIN wde_tables w ON a.tbl = w.tbl WHERE w.tbl IS NULL) as only_in_active_webi_source,
# MAGIC   (SELECT count(*) FROM wde_tables w LEFT JOIN aws_tables a ON a.tbl = w.tbl WHERE a.tbl IS NULL) as only_in_webi_data_entries

# COMMAND ----------

# DBTITLE 1,Document coverage gap
# MAGIC %sql
# MAGIC -- Document coverage gap
# MAGIC SELECT 
# MAGIC   (SELECT count(distinct Document_Id) FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_source) as aws_docs,
# MAGIC   (SELECT count(distinct Document_Id) FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.webi_data_entries) as wde_docs,
# MAGIC   (SELECT count(distinct a.Document_Id) 
# MAGIC    FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_source a
# MAGIC    LEFT JOIN (SELECT DISTINCT Document_Id FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.webi_data_entries) b
# MAGIC      ON a.Document_Id = b.Document_Id
# MAGIC    WHERE b.Document_Id IS NULL) as aws_docs_NOT_in_wde,
# MAGIC   (SELECT count(distinct b.Document_Id) 
# MAGIC    FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.webi_data_entries b
# MAGIC    LEFT JOIN (SELECT DISTINCT Document_Id FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_source) a
# MAGIC      ON a.Document_Id = b.Document_Id
# MAGIC    WHERE a.Document_Id IS NULL) as wde_docs_NOT_in_aws

# COMMAND ----------

# DBTITLE 1,Sample tables only in webi_data_entries (potential complement)
# MAGIC %sql
# MAGIC -- Tables in webi_data_entries that are NOT in active_webi_source (potential enrichment)
# MAGIC WITH aws_tables AS (
# MAGIC   SELECT DISTINCT upper(trim(table_Name)) as tbl FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.active_webi_source
# MAGIC   WHERE table_Name IS NOT NULL
# MAGIC ),
# MAGIC wde_tables AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     upper(trim(split(sql_definition, '\\.')[0])) as tbl,
# MAGIC     count(distinct Document_Id) as doc_cnt
# MAGIC   FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.webi_data_entries
# MAGIC   WHERE sql_definition IS NOT NULL AND sql_definition LIKE '%.%'
# MAGIC   GROUP BY upper(trim(split(sql_definition, '\\.')[0]))
# MAGIC )
# MAGIC SELECT w.tbl as table_name_from_universe, w.doc_cnt as reports_using_it
# MAGIC FROM wde_tables w
# MAGIC LEFT JOIN aws_tables a ON a.tbl = w.tbl
# MAGIC WHERE a.tbl IS NULL
# MAGIC ORDER BY w.doc_cnt DESC
# MAGIC LIMIT 30

# COMMAND ----------

# DBTITLE 1,Column mapping comparison
# MAGIC %sql
# MAGIC -- Column mapping: what active_webi_source provides vs what webi_data_entries can offer
# MAGIC -- active_webi_source key columns used in the lineage pipeline:
# MAGIC --   Document_Id, Document_CUID, Document_name, Full_path, updated_by,
# MAGIC --   source_DB_connection, sql_table, table_Name, schema_Name
# MAGIC --
# MAGIC -- webi_data_entries closest equivalents:
# MAGIC --   Document_Id, Document_name, DataSource_Name (≈ source_DB_connection?),
# MAGIC --   sql_definition (contains TABLE.COLUMN — need to split),
# MAGIC --   Universe_Name, UNIVERSE_PRIMARY_CATEGORY
# MAGIC --   MISSING: Document_CUID, Full_path, updated_by, schema_Name (separate from table)
# MAGIC
# MAGIC SELECT 
# MAGIC   Document_Id,
# MAGIC   Document_name,
# MAGIC   DataSource_Name,
# MAGIC   Universe_Name,
# MAGIC   sql_definition,
# MAGIC   upper(trim(split(sql_definition, '\\.')[0])) as extracted_table,
# MAGIC   UNIVERSE_PRIMARY_CATEGORY
# MAGIC FROM dataplatform01_central_dev_catalog_01.custom_sap_bo.webi_data_entries
# MAGIC WHERE sql_definition IS NOT NULL AND sql_definition LIKE '%.%'
# MAGIC LIMIT 20

# COMMAND ----------


