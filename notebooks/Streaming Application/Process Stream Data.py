# Databricks notebook source
from pyspark.sql.column import *
from pyspark.sql.dataframe import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# MAGIC %run "./../Configuration/App Config"

# COMMAND ----------

# DBTITLE 1,Create DeltaTable Objects
CustomerDeltaTable = DeltaTable.forPath(spark, CUSTOMER_DELTA )
AddressDeltaTable = DeltaTable.forPath(spark, ADDRESS_DELTA)
CustomerAddressDeltaTable = DeltaTable.forPath(spark, CUSTOMER_ADDRESS_DELTA)
ProductDeltaTable = DeltaTable.forPath(spark, PRODUCT_DELTA)
ProductCategoryDeltaTable = DeltaTable.forPath(spark, PRODUCT_CATEGORGY_DELTA)
ProductDescriptionDeltaTable = DeltaTable.forPath(spark, PRODUCT_DESCRIPTION_DELTA)
ProductModelDeltaTable = DeltaTable.forPath(spark, PRODUCT_MODEL_DELTA)
ProductModelProductDescriptionDeltaTable = DeltaTable.forPath(spark, PRODUCT_MODEL_PRODUCT_DESCRIPTION_DELTA)
SalesOrderDetailDeltaTable = DeltaTable.forPath(spark, SALES_ORDER_DETAIL_DELTA)
SalesOrderHeaderDeltaTable = DeltaTable.forPath(spark, SALES_ORDER_HEADER_DELTA)

# COMMAND ----------

# DBTITLE 1,Define Schemas
customer_schema = CustomerDeltaTable.toDF().schema
address_schema = AddressDeltaTable.toDF().schema
customer_address_schema = CustomerAddressDeltaTable.toDF().schema
product_schema = ProductDeltaTable.toDF().schema
product_category_schema = ProductCategoryDeltaTable.toDF().schema
product_description_schema = ProductDescriptionDeltaTable.toDF().schema
product_model_schema = ProductModelDeltaTable.toDF().schema
product_model_product_description_schema = ProductModelProductDescriptionDeltaTable.toDF().schema
sales_order_detail_schema = SalesOrderDetailDeltaTable.toDF().schema
sales_order_header_schema = SalesOrderHeaderDeltaTable.toDF().schema

# COMMAND ----------

def process_incoming_data(batch_df, batchId):
  
  # Transform the incoming avro to a structured format
  transformed_df = (batch_df
                    .withColumn("Schema", col("Properties").getItem("__$Schema").getItem("member2"))
                    .withColumn("Table", col("Properties").getItem("__$Table").getItem("member2"))
                    .drop(col("Properties"))
                    .select(col("Body").cast("string"), col('Schema'), col('Table'), col("EnqueuedTimeUtc"))
                    .withColumn('Customer', 
                                when(col('Table')=='Customer', from_json('Body', customer_schema))
                                .otherwise(None)
                               )
                    .withColumn('Address', 
                                when(col('Table')=='Address', from_json('Body', address_schema))
                                .otherwise(None)
                               )
                    .withColumn('CustomerAddress', 
                                when(col('Table')=='CustomerAddress', from_json('Body', customer_address_schema))
                                .otherwise(None)
                               )
                    .withColumn('Product', 
                                when(col('Table')=='Product', from_json('Body', product_schema))
                                .otherwise(None)
                               )
                    .withColumn('ProductCategory', 
                                when(col('Table')=='ProductCategory', from_json('Body', product_category_schema))
                                .otherwise(None)
                               )
                    .withColumn('ProductDescription', 
                                when(col('Table')=='ProductDescription', from_json('Body', product_description_schema))
                                .otherwise(None)
                               )
                    .withColumn('ProductModel', 
                                when(col('Table')=='ProductModel', from_json('Body', product_model_schema))
                                .otherwise(None)
                               )
                    .withColumn('ProductModelProductDescription', 
                                when(col('Table')=='ProductModelProductDescription', from_json('Body', product_model_product_description_schema))
                                .otherwise(None)
                               )
                    .withColumn('SalesOrderDetail', 
                                when(col('Table')=='SalesOrderDetail', from_json('Body', sales_order_detail_schema))
                                .otherwise(None)
                               )
                    .withColumn('SalesOrderHeader', 
                                when(col('Table')=='SalesOrderHeader', from_json('Body', sales_order_header_schema))
                                .otherwise(None)
                               )
                  )
  
  # cache the transformed dataframe 
  transformed_df.cache()
  transformed_df.count()
  
  # filter based on table name and write contents to the specific table 
#   transformed_df.write.format(...).save(...)  // customer 1
#   transformed_df.write.format(...).save(...)  // address 2
#   transformed_df.write.format(...).save(...)  // customer_address 3
#   transformed_df.write.format(...).save(...)  // product 4
#   transformed_df.write.format(...).save(...)  // product_category 5  
#   transformed_df.write.format(...).save(...)  // product_description 6  
#   transformed_df.write.format(...).save(...)  // product_model 7  
#   transformed_df.write.format(...).save(...)  // product_model_product_description 8
#   transformed_df.write.format(...).save(...)  // sales_order_detail 9
#   transformed_df.write.format(...).save(...)  // sales_order_header 10
  
  # clear cache
  transformed_df.unpersist()
  