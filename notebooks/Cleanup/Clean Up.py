# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook to tear down database

# COMMAND ----------

# MAGIC %run "./../Configuration/App Config"

# COMMAND ----------

spark.sql("DROP DATABASE IF EXISTS {table_name} CASCADE".format(table_name="StructuredStreaming"))

# COMMAND ----------

dbutils.fs.rm(ADDRESS_DELTA, True)

# COMMAND ----------

dbutils.fs.rm(CUSTOMER_DELTA, True)

# COMMAND ----------

dbutils.fs.rm(CUSTOMER_ADDRESS_DELTA , True)

# COMMAND ----------

dbutils.fs.rm(PRODUCT_DELTA, True)

# COMMAND ----------

dbutils.fs.rm(PRODUCT_CATEGORGY_DELTA , True)

# COMMAND ----------

dbutils.fs.rm(PRODUCT_DESCRIPTION_DELTA, True)

# COMMAND ----------

dbutils.fs.rm(PRODUCT_MODEL_DELTA, True)

# COMMAND ----------

dbutils.fs.rm(PRODUCT_MODEL_PRODUCT_DESCRIPTION_DELTA , True)

# COMMAND ----------

dbutils.fs.rm(SALES_ORDER_DETAIL_DELTA, True)

# COMMAND ----------

dbutils.fs.rm(SALES_ORDER_HEADER_DELTA, True)