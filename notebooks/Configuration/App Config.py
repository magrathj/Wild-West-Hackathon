# Databricks notebook source
storageAccount = dbutils.secrets.get(scope="kv-hackathon-bp", key="ADLS--Account")
storageKey = dbutils.secrets.get(scope="kv-hackathon-bp", key="ADLS--Key")

spark.conf.set(f"fs.azure.account.key.{storageAccount}.blob.core.windows.net", storageKey)
spark.conf.set(f"fs.azure.account.key.{storageAccount}.dfs.core.windows.net", storageKey)

BASE_DIRECTORY = f"abfss://lake@{storageAccount}.dfs.core.windows.net"
TMP_DIR = f"wasbs://lake@{storageAccount}.blob.core.windows.net/tmp"

inputDirectory = BASE_DIRECTORY + "/zone=raw/subjectarea=hackathon/ehn-hackathon-bp/*/*/*/*/*/*"

# COMMAND ----------

TMP_DIR

# COMMAND ----------

inputDirectory

# COMMAND ----------

dbutils.fs.ls(BASE_DIRECTORY)

# COMMAND ----------

ADDRESS = BASE_DIRECTORY + '/address.parquet'
CUSTOMER = BASE_DIRECTORY + '/customer.parquet'
CUSTOMER_ADDRESS = BASE_DIRECTORY + '/customeraddress.parquet'
PRODUCT = BASE_DIRECTORY + '/product.parquet'
PRODUCT_CATEGORGY = BASE_DIRECTORY + '/productcategory.parquet'
PRODUCT_DESCRIPTION = BASE_DIRECTORY + '/productdescription.parquet'
PRODUCT_MODEL = BASE_DIRECTORY + '/productmodel.parquet'
PRODUCT_MODEL_PRODUCT_DESCRIPTION = BASE_DIRECTORY + '/productmodelproductdescription.parquet'
SALES_ORDER_DETAIL = BASE_DIRECTORY + '/salesorderdetail.parquet'
SALES_ORDER_HEADER = BASE_DIRECTORY + '/salesorderheader.parquet'

# COMMAND ----------



# COMMAND ----------

display(spark.read.format("parquet").option("inferSchema", True).load(SALES_ORDER_HEADER))

# COMMAND ----------

schema.simpleString()

# COMMAND ----------

schema.needConversion()