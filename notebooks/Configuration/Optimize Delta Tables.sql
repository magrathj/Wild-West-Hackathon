-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Optimize delta tables

-- COMMAND ----------

OPTIMIZE StructuredStreaming.SalesOrderDetail



-- COMMAND ----------

OPTIMIZE StructuredStreaming.SalesOrderHeader



-- COMMAND ----------

OPTIMIZE StructuredStreaming.Customer



-- COMMAND ----------

OPTIMIZE StructuredStreaming.Address



-- COMMAND ----------

OPTIMIZE StructuredStreaming.Product



-- COMMAND ----------

OPTIMIZE StructuredStreaming.ProductDescription



-- COMMAND ----------

OPTIMIZE StructuredStreaming.ProductCategory



-- COMMAND ----------

OPTIMIZE StructuredStreaming.ProductModel



-- COMMAND ----------

OPTIMIZE StructuredStreaming.ProductModelProductDescription



-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.notebook.exit("Success")