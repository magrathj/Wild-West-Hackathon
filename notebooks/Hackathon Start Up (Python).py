# Databricks notebook source
# MAGIC %md 
# MAGIC # Welcome to the Hackathon
# MAGIC 
# MAGIC This notebook can get you started down the path to streaming data into Databricks Delta.  There are other notebooks in your folder, so feel free to check them out as you will probably need them later.  Once you figure out what the data looks like, you will need to build out the schemas in the "Build Delta Tables" notebook.  To keep your Delta tables running well, you might need to occasionally optimize them.  Complete the "Optimize Delta Tables" notebook to keep you tables as efficient as possible.

# COMMAND ----------

# MAGIC %md ## Run Hackathon Common
# MAGIC This just sets up some connections to the datalake.  You will want to make use of the TMP_DIR and inputDirectory variables in your notebook though.

# COMMAND ----------

# MAGIC %run "./Hackathon Common (Python)"

# COMMAND ----------

# MAGIC %md ### Create a Checkpoint Location for your team

# COMMAND ----------

checkpointLocation = TMP_DIR + "/Structured_Streaming/_chkp"
checkpointLocation

# COMMAND ----------

# MAGIC %md ## Imports
# MAGIC Pretty standard stuff here

# COMMAND ----------

from pyspark.sql.column import *
from pyspark.sql.dataframe import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# MAGIC %md ## Build Input File Schema
# MAGIC Ugh, you are going to want this.

# COMMAND ----------

schema = StructType([
  StructField("EnqueuedTimeUtc", StringType(), True),
  StructField("Body", BinaryType(), True),
  StructField("Properties", MapType(StringType(), StructType([
    StructField("member0", LongType(), True),
    StructField("member1", DoubleType(), True),
    StructField("member2", StringType(), True),
    StructField("member3", BinaryType(), True)
  ])), True)
])

# COMMAND ----------

# MAGIC %md ## Test Read
# MAGIC Well, we better see if we can get to the data.

# COMMAND ----------

rawDF = spark.read.format("avro")\
  .option("inferschema", True)\
  .load(inputDirectory)

display(rawDF)

# COMMAND ----------

rawDF = spark.read.format("avro")\
  .option("inferschema", False)\
  .schema(schema)\
  .load(inputDirectory)

display(rawDF)

# COMMAND ----------

# MAGIC %md There is the data, but it looks kind of rough.  It certainly doesn't look like data that I would expect from a store.  To make this data useful, we are going to need to do some data engineering work.  Perhaps the first step is figuring out what kind of data we are getting in each record.  The Properties column might give us a clue there (see Hint #1 if you need help).  After that, we need to figure out how to get the actual record from the Body column.  I'm pretty the body needs to be converted to a string (see Hint #2 if you need help).

# COMMAND ----------

# MAGIC %md ### Hint #1

# COMMAND ----------

reshapedDF = rawDF\
  .withColumn("__$Schema", col("Properties").getItem("__$Schema").getItem("member2"))\
  .withColumn("__$Table", col("Properties").getItem("__$Table").getItem("member2"))\
  .drop(col("EnqueuedTimeUtc"))\
  .drop(col("Body"))\
  .drop(col("Properties"))

display(reshapedDF.distinct())

# COMMAND ----------

display(rawDF
        .withColumn("__$Schema", col("Properties").getItem("__$Schema").getItem("member2"))
        .withColumn("__$Table", col("Properties").getItem("__$Table").getItem("member1"))
        .withColumn("__$Table2", col("Properties").getItem("__$Table").getItem("member2"))
        .withColumn("__$Table3", col("Properties").getItem("__$Table").getItem("member3"))
       )

# COMMAND ----------

# MAGIC %md ### Hint #2

# COMMAND ----------

display(rawDF.select(col("Body").cast("string"), col("Properties")))

# COMMAND ----------

display(
  rawDF
  .withColumn("Schema", col("Properties").getItem("__$Schema").getItem("member2"))
  .withColumn("Table", col("Properties").getItem("__$Table").getItem("member2"))
  .drop(col("Properties"))
  .select(col("Body").cast("string"), col('Schema'), col('Table'), col("EnqueuedTimeUtc"))
 # .withColumn("Body", col("Body").getItem("__$Table").getItem("member2"))
)

# COMMAND ----------

# MAGIC %md You're going to have to take it from here.  You might want to first get a handle on the data before you work on setting up structured streaming, but it is really up to you.  Don't forget to setup your Team's database and Delta tables.

# COMMAND ----------

display(
  rawDF
  .withColumn("Schema", explode(col("Properties").getItem("__$Table")))
)

# COMMAND ----------

# MAGIC %md ## Return Success

# COMMAND ----------

dbutils.notebook.exit("Success")