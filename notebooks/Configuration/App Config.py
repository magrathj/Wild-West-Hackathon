# Databricks notebook source
# MAGIC %md
# MAGIC Set up connection to blob store

# COMMAND ----------

storageAccount = dbutils.secrets.get(scope="kv-hackathon-bp", key="ADLS--Account")
storageKey = dbutils.secrets.get(scope="kv-hackathon-bp", key="ADLS--Key")

spark.conf.set(f"fs.azure.account.key.{storageAccount}.blob.core.windows.net", storageKey)
spark.conf.set(f"fs.azure.account.key.{storageAccount}.dfs.core.windows.net", storageKey)

BASE_DIRECTORY = f"abfss://lake@{storageAccount}.dfs.core.windows.net"
TMP_DIR = f"wasbs://lake@{storageAccount}.blob.core.windows.net/tmp"

TEAM_DIR = TMP_DIR + '/StructuredStreaming/delta'

inputDirectory = BASE_DIRECTORY + "/zone=raw/subjectarea=hackathon/ehn-hackathon-bp/*/*/*/*/*/*"

DB_PATH = "dbfs:/user/hive/warehouse/structuredstreaming.db/"

# COMMAND ----------

TMP_DIR

# COMMAND ----------

inputDirectory

# COMMAND ----------

checkpointLocation = TMP_DIR + "/Structured_Streaming/_chkp"
checkpointLocation

# COMMAND ----------

# MAGIC %md
# MAGIC Parquet Paths

# COMMAND ----------

ADDRESS_PARQUET = BASE_DIRECTORY + '/address.parquet'
CUSTOMER_PARQUET = BASE_DIRECTORY + '/customer.parquet'
CUSTOMER_ADDRESS_PARQUET = BASE_DIRECTORY + '/customeraddress.parquet'
PRODUCT_PARQUET = BASE_DIRECTORY + '/product.parquet'
PRODUCT_CATEGORGY_PARQUET = BASE_DIRECTORY + '/productcategory.parquet'
PRODUCT_DESCRIPTION_PARQUET = BASE_DIRECTORY + '/productdescription.parquet'
PRODUCT_MODEL_PARQUET = BASE_DIRECTORY + '/productmodel.parquet'
PRODUCT_MODEL_PRODUCT_DESCRIPTION_PARQUET = BASE_DIRECTORY + '/productmodelproductdescription.parquet'
SALES_ORDER_DETAIL_PARQUET = BASE_DIRECTORY + '/salesorderdetail.parquet'
SALES_ORDER_HEADER_PARQUET = BASE_DIRECTORY + '/salesorderheader.parquet'

# COMMAND ----------

# MAGIC %md
# MAGIC * ADDRESS_PARQUET 
# MAGIC * CUSTOMER_PARQUET 
# MAGIC * CUSTOMER_ADDRESS_PARQUET 
# MAGIC * PRODUCT_PARQUET 
# MAGIC * PRODUCT_CATEGORGY_PARQUET 
# MAGIC * PRODUCT_DESCRIPTION_PARQUET 
# MAGIC * PRODUCT_MODEL_PARQUET 
# MAGIC * PRODUCT_MODEL_PRODUCT_DESCRIPTION_PARQUET 
# MAGIC * SALES_ORDER_DETAIL_PARQUET 
# MAGIC * SALES_ORDER_HEADER_PARQUET  

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Paths

# COMMAND ----------

ADDRESS_DELTA = DB_PATH + '/address'
CUSTOMER_DELTA = DB_PATH + '/customer'
CUSTOMER_ADDRESS_DELTA = DB_PATH + '/customeraddress'
PRODUCT_DELTA = DB_PATH + '/product'
PRODUCT_CATEGORGY_DELTA = DB_PATH + '/productcategory'
PRODUCT_DESCRIPTION_DELTA = DB_PATH + '/productdescription'
PRODUCT_MODEL_DELTA = DB_PATH + '/productmodel'
PRODUCT_MODEL_PRODUCT_DESCRIPTION_DELTA = DB_PATH + '/productmodelproductdescription'
SALES_ORDER_DETAIL_DELTA = DB_PATH + '/salesorderdetail'
SALES_ORDER_HEADER_DELTA = DB_PATH + '/salesorderheader'

# COMMAND ----------

# MAGIC %md
# MAGIC * ADDRESS_DELTA
# MAGIC * CUSTOMER_DELTA 
# MAGIC * CUSTOMER_ADDRESS_DELTA 
# MAGIC * PRODUCT_DELTA
# MAGIC * PRODUCT_CATEGORGY_DELTA 
# MAGIC * PRODUCT_DESCRIPTION_DELTA
# MAGIC * PRODUCT_MODEL_DELTA 
# MAGIC * PRODUCT_MODEL_PRODUCT_DESCRIPTION_DELTA 
# MAGIC * SALES_ORDER_DETAIL_DELTA 
# MAGIC * SALES_ORDER_HEADER_DELTA

# COMMAND ----------

