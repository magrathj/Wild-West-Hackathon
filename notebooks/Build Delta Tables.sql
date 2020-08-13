-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Create Your Team's Database
-- MAGIC - No Spaces
-- MAGIC - Recommend Pascal Casing (LikeThisForExample)

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS <YOUR TEAM NAME HERE>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Create SalesOrderDetail Delta Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS <YOUR TEAM NAME HERE>.SalesOrderDetail (
  LineTotal DOUBLE,
  ModifiedDate STRING,
  OrderQty LONG,
  ProductID LONG,
  SalesOrderDetailID LONG,
  SalesOrderID LONG,
  UnitPrice DOUBLE,
  UnitPriceDiscount DOUBLE,
  rowguid STRING
)
USING DELTA

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Create SalesOrderHeader Delta Table

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md ## Create Customer Delta Table

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md ## Create Address Delta Table

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md ## Create CustomerAddress Delta Table

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md ## Create Product Delta Table

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md ## Create ProductDescription Delta Table

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md ## Create ProductCategory Delta Table

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md ## Create ProductModel Delta Table

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md ## Create ProductModelProductDescription Delta Table

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.notebook.exit("Success")