# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook to tear down database

# COMMAND ----------

# MAGIC %run "./../Configuration/App Config"

# COMMAND ----------

spark.sql("DROP DATABASE IF EXISTS {table_name} CASCADE".format(table_name="StructuredStreaming"))

# COMMAND ----------

dbutils.fs.ls(TEAM_DIR )

# COMMAND ----------

dbutils.fs.rm(TEAM_DIR , True)