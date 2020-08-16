# Databricks notebook source
avro_schema = StructType([
  StructField("EnqueuedTimeUtc", StringType(), True),
  StructField("Body", BinaryType(), True),
  StructField("Properties", MapType(StringType(), StructType([
    StructField("member0", LongType(), True),
    StructField("member1", DoubleType(), True),
    StructField("member2", StringType(), True),
    StructField("member3", BinaryType(), True)
  ])), True)
])