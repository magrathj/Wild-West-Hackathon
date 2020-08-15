# Databricks notebook source
from pyspark.sql.column import *
from pyspark.sql.dataframe import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# MAGIC %run "./../Configuration/App Config"

# COMMAND ----------

# MAGIC %run "./../Setup/Schemas"

# COMMAND ----------

# DBTITLE 1,Read in Stream
stream_df = (spark.readStream.format("avro")
            .option("inferschema", False)
            .schema(schema)
            .load(inputDirectory)
           )

# COMMAND ----------

(
  stream_df
  .outputMode("append")
  .option("checkpointLocation", checkpointLocation)
  .format("memory")
)