-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Configure auto-optimization and auto-compaction of delta tables

-- COMMAND ----------

ALTER TABLE StructuredStreaming.SalesOrderDetail SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

-- COMMAND ----------

ALTER TABLE StructuredStreaming.SalesOrderHeader SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

-- COMMAND ----------

ALTER TABLE StructuredStreaming.Customer SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

-- COMMAND ----------

ALTER TABLE StructuredStreaming.Address SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

-- COMMAND ----------

ALTER TABLE StructuredStreaming.Product SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

-- COMMAND ----------

ALTER TABLE StructuredStreaming.ProductDescription SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

-- COMMAND ----------

ALTER TABLE StructuredStreaming.ProductCategory SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

-- COMMAND ----------

ALTER TABLE StructuredStreaming.ProductModel SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

-- COMMAND ----------

ALTER TABLE StructuredStreaming.ProductModelProductDescription SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.notebook.exit("Success")