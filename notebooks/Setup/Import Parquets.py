# Databricks notebook source
# MAGIC %md
# MAGIC ### Import Parquet data into the delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC Using the parquet files in the TMP_DIR to re-populate the delta tables 

# COMMAND ----------

# MAGIC %run "./../Configuration/App Config"

# COMMAND ----------

def import_parquet_files(tables_info):
  for table_info in tables_info: 
    print(f" import parquet file at {table_info['parquet_path']} into { table_info['table_name']}")
    spark.read.format("parquet").option("inferSchema", True).load(table_info['parquet_path']).write.format("delta").option("mergeSchema", False).save(table_info['table_name'])

# COMMAND ----------

# DBTITLE 1,Define the paths and corresponding delta tables
parquet_files_to_insert = [
  {
    'table_name' : 'StructuredStreaming.Address',
    'parquet_path' : ADDRESS
  }
#   ,
#   {
#     'table_name' : '',
#     'parquet_path' : ADDRESS
#   },
#   {
#     'table_name' : '',
#     'parquet_path' : ADDRESS
#   },
#   {
#     'table_name' : '',
#     'parquet_path' : ADDRESS
#   },
#   {
#     'table_name' : '',
#     'parquet_path' : ADDRESS
#   },
#   {
#     'table_name' : '',
#     'parquet_path' : ADDRESS
#   },
#   {
#     'table_name' : '',
#     'parquet_path' : ADDRESS
#   },
#   {
#     'table_name' : '',
#     'parquet_path' : ADDRESS
#   },
]

# COMMAND ----------

import_parquet_files(parquet_files_to_insert) 

# COMMAND ----------

