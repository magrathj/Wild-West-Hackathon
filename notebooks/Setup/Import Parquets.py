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
    try:
      print(f" imported parquet file at: {table_info['parquet_path']} into: { table_info['table_path']}")
      (spark.read.format("parquet")
        .option("inferSchema", True)
        .load(table_info['parquet_path'])
        .write.format("delta")
        .option("mergeSchema", False)
        .mode("append")
        .save(table_info['table_path'])
      )
    except Exception as error:
      print(f" parquet file at: {table_info['parquet_path']} did not get import")
      print(f"error: {error}")    

# COMMAND ----------

# DBTITLE 1,Define the paths and corresponding delta tables
parquet_files_to_insert = [
  {
    'table_name' : 'address',
    'table_path' : ADDRESS_DELTA,
    'parquet_path' : ADDRESS_PARQUET
  },
  {
    'table_name' : 'customer',
    'table_path' : CUSTOMER_DELTA,
    'parquet_path' : CUSTOMER_PARQUET
  },
  {
    'table_name' : 'customeraddress',
    'table_path' : CUSTOMER_ADDRESS_DELTA,
    'parquet_path' : CUSTOMER_ADDRESS_PARQUET
  },
  {
    'table_name' : 'product',
    'table_path' : PRODUCT_DELTA,
    'parquet_path' : PRODUCT_PARQUET
  },
  {
    'table_name' : 'productcategory',
    'table_path' : PRODUCT_CATEGORGY_DELTA,
    'parquet_path' : PRODUCT_CATEGORGY_PARQUET
  },
  {
    'table_name' : 'productdescription',
    'table_path' : PRODUCT_DESCRIPTION_DELTA,
    'parquet_path' : PRODUCT_DESCRIPTION_PARQUET
  },
  {
    'table_name' : 'productmodel',
    'table_path' : PRODUCT_MODEL_DELTA,
    'parquet_path' : PRODUCT_MODEL_PARQUET
  },
  {
    'table_name' : 'productmodelproductdescription',
    'table_path' : PRODUCT_MODEL_PRODUCT_DESCRIPTION_DELTA,
    'parquet_path' : PRODUCT_MODEL_PRODUCT_DESCRIPTION_PARQUET
  },
  {
    'table_name' : 'salesorderdetail',
    'table_path' : SALES_ORDER_DETAIL_DELTA,
    'parquet_path' : SALES_ORDER_DETAIL_PARQUET
  },
  {
    'table_name' : 'salesorderheader',
    'table_path' : SALES_ORDER_HEADER_DELTA,
    'parquet_path' : SALES_ORDER_HEADER_PARQUET
  }
]

# COMMAND ----------

import_parquet_files(parquet_files_to_insert) 

# COMMAND ----------

