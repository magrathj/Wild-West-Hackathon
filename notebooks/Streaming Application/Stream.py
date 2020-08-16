# Databricks notebook source
from pyspark.sql.column import *
from pyspark.sql.dataframe import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# MAGIC %run "./../Setup/Schemas"

# COMMAND ----------

# MAGIC %run "./Process Stream Data"

# COMMAND ----------

# DBTITLE 1,Read in Stream
stream_df = spark.readStream.format("avro") \
              .option("inferschema", False) \
              .schema(avro_schema) \
              .option("maxFilesPerTrigger", "1") \
              .load(inputDirectory)

# COMMAND ----------

# DBTITLE 1,Merge to specific tables using micro batch 
stream_df.writeStream \
  .format("delta") \
  .foreachBatch(process_incoming_data) \
  .outputMode("update") \
  .option("checkpointLocation", checkpointLocation) \
  .start() 

# COMMAND ----------

