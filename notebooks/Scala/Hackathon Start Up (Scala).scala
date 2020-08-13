// Databricks notebook source
// MAGIC %md 
// MAGIC # Welcome to the Hackathon
// MAGIC 
// MAGIC This notebook can get you started down the path to streaming data into Databricks Delta.  There are other notebooks in your folder, so feel free to check them out as you will probably need them later.  Once you figure out what the data looks like, you will need to build out the schemas in the "Build Delta Tables" notebook.  To keep your Delta tables running well, you might need to occasionally optimize them.  Complete the "Optimize Delta Tables" notebook to keep you tables as efficient as possible.

// COMMAND ----------

// MAGIC %md ## Run Hackathon Common
// MAGIC This just sets up some connections to the datalake.  You will want to make use of the TMP_DIR and inputDirectory variables in your notebook though.

// COMMAND ----------

// MAGIC %run "./Hackathon Common (Scala)"

// COMMAND ----------

// MAGIC %md ### Create a Checkpoint Location for your team
// MAGIC A checkpoint helps keep your stream from needing to reprocess everything in the event that you have to restart your notebook

// COMMAND ----------

//val checkpointLocation = TMP_DIR + "/<YOUR TEAM NAME>"

// COMMAND ----------

// MAGIC %md ## Imports
// MAGIC Pretty standard stuff here

// COMMAND ----------

// Scala or Java Imports

// Spark Imports
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, ArrayType, DataType}
import spark.implicits._

// Custom or 3rd Party Imports
import io.delta.tables._

// COMMAND ----------

// MAGIC %md ## Build Input File Schema
// MAGIC Ugh, you are going to want this.

// COMMAND ----------

val schema = StructType(Array(
    StructField("EnqueuedTimeUtc", StringType, true),
    StructField("Body", BinaryType, true),
    StructField("Properties", MapType(StringType, 
        StructType(Array(
                    StructField("member0", LongType, true),            
                    StructField("member1", DoubleType, true),            
                    StructField("member2", StringType, true),            
                    StructField("member3", BinaryType, true)
                  ))
       ), true)
  ))

// COMMAND ----------

// MAGIC %md ## Test Read
// MAGIC Well, we better see if we can get to the data.

// COMMAND ----------

val rawDF = spark
    .read
    .option("inferschema", false)
    .schema(schema)
    .format("avro")
    .load(inputDirectory)

display(rawDF)

// COMMAND ----------

// MAGIC %md There is the data, but it looks kind of rough.  It certainly doesn't look like data that I would expect from a store.  To make this data useful, we are going to need to do some data engineering work.  Perhaps the first step is figuring out what kind of data we are getting in each record.  The Properties column might give us a clue there (see Hint #1 if you need help).  After that, we need to figure out how to get the actual record from the Body column.  I'm pretty the body needs to be converted to a string (see Hint #2 if you need help).

// COMMAND ----------

// MAGIC %md ### Hint #1

// COMMAND ----------

val reshapedDF = rawDF
  .withColumn("__$Schema", col("Properties").getItem("__$Schema").getItem("member2"))
  .withColumn("__$Table", col("Properties").getItem("__$Table").getItem("member2"))
  .drop(col("EnqueuedTimeUtc"))
  .drop(col("Body"))
  .drop(col("Properties"))

display(reshapedDF.distinct)

// COMMAND ----------

// MAGIC %md ### Hint #2

// COMMAND ----------

display(rawDF.select(col("Body").cast("string"), col("Properties")))

// COMMAND ----------

// MAGIC %md You're going to have to take it from here.  You might want to first get a handle on the data before you work on setting up structured streaming, but it is really up to you.  Don't forget to setup your Team's database and Delta tables.

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ## Return Success

// COMMAND ----------

dbutils.notebook.exit("Success")