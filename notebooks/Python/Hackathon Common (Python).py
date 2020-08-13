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