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

TEAM_DIR = TMP_DIR + '/StructuredStreaming'

inputDirectory = BASE_DIRECTORY + "/zone=raw/subjectarea=hackathon/ehn-hackathon-bp/*/*/*/*/*/*"

# COMMAND ----------

TMP_DIR

# COMMAND ----------

inputDirectory

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
# MAGIC Delta Paths

# COMMAND ----------

ADDRESS_DELTA = TEAM_DIR + '/address'
CUSTOMER_DELTA = TEAM_DIR + '/customer'
CUSTOMER_ADDRESS_DELTA = TEAM_DIR + '/customeraddress'
PRODUCT_DELTA = TEAM_DIR + '/product'
PRODUCT_CATEGORGY_DELTA = TEAM_DIR + '/productcategory'
PRODUCT_DESCRIPTION_DELTA = TEAM_DIR + '/productdescription'
PRODUCT_MODEL_DELTA = TEAM_DIR + '/productmodel'
PRODUCT_MODEL_PRODUCT_DESCRIPTION_DELTA = TEAM_DIR + '/productmodelproductdescription'
SALES_ORDER_DETAIL_DELTA = TEAM_DIR + '/salesorderdetail'
SALES_ORDER_HEADER_DELTA = TEAM_DIR + '/salesorderheader'