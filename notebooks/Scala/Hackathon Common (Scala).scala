// Databricks notebook source
val storageAccount = dbutils.secrets.get(scope="kv-hackathon-bp", key="ADLS--Account")
var storageKey = dbutils.secrets.get(scope="kv-hackathon-bp", key="ADLS--Key")

spark.conf.set(s"fs.azure.account.key.${storageAccount}.blob.core.windows.net", storageKey)
spark.conf.set(s"fs.azure.account.key.${storageAccount}.dfs.core.windows.net", storageKey)

val BASE_DIRECTORY = s"abfss://lake@${storageAccount}.dfs.core.windows.net"
val TMP_DIR = s"wasbs://lake@${storageAccount}.blob.core.windows.net/tmp"

val inputDirectory = BASE_DIRECTORY + "/zone=raw/subjectarea=hackathon/ehn-hackathon-bp/*/*/*/*/*/*"