# Databricks notebook source
from pyspark.sql.column import *
from pyspark.sql.dataframe import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# MAGIC %run "./../Configuration/App Config"

# COMMAND ----------

def process_incoming_data(batch_df, batchId):
  
  incoming_data = batch_df
  incoming_data.persist()
  incoming_data.count()
#   batchDF.write.format(...).save(...)  // location 1
#   batchDF.write.format(...).save(...)  // location 2
#   batchDF.write.format(...).save(...)  // location 3
#   batchDF.write.format(...).save(...)  // location 4
#   batchDF.write.format(...).save(...)  // location 5  
#   batchDF.write.format(...).save(...)  // location 6  
#   batchDF.write.format(...).save(...)  // location 7  
#   batchDF.write.format(...).save(...)  // location 8
  incoming_data.unpersist()
  