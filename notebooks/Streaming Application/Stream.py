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
# stream_df = (spark.readStream.format("avro")
#             .option("inferschema", False)
#             .schema(schema)
#             .load(inputDirectory)
#            )

# COMMAND ----------

# query = stream_df \
#     .writeStream \
#     .outputMode("append") \
#     .queryName("structuredstreaming") \
#     .format("memory") \
#     .start()

# query.awaitTermination()

# COMMAND ----------

stream_df = spark.readStream.format("avro") \
              .option("inferschema", False) \
              .schema(avro_schema) \
              .option("maxFilesPerTrigger", "1") \
              .load(inputDirectory)

# COMMAND ----------

stream_df.writeStream \
  .format("delta") \
  .foreachBatch(process_incoming_data) \
  .outputMode("update") \
  .start() 