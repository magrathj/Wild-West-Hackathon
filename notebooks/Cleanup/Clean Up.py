# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook to tear down database

# COMMAND ----------

spark.sql("DROP DATABASE IF EXISTS {table_name} CASCADE".format(table_name="StructuredStreaming"))