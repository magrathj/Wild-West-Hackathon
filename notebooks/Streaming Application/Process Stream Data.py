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

# DBTITLE 1,Micro batch function to transform, filter and merge data into delta
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
  print(transformed_df.count())
  
  # filter based on table name and write contents to the specific table 
  CustomerDeltaTable.alias("table") \
    .merge(transformed_df.filter('Table=="Customer"').select("Customer.*").alias("dataframe"),
           "table.CustomerID = dataframe.CustomerID") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()  # // customer 1
  
  AddressDeltaTable.alias("table") \
    .merge(transformed_df.filter('Table=="Address"').select("Address.*").alias("dataframe"),
           "table.AddressID = dataframe.AddressID") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()  #// address 2
  
  CustomerAddressDeltaTable.alias("table") \
    .merge(transformed_df.filter('Table=="CustomerAddress"').select("CustomerAddress.*").alias("dataframe"),
           "table.CustomerID = dataframe.CustomerID and table.AddressID = dataframe.AddressID") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()  #// customer_address 3
  
  ProductDeltaTable.alias("table") \
     .merge(transformed_df.filter('Table=="Product"').select("Product.*").alias("dataframe"),
            "table.ProductID = dataframe.ProductID") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()  #// product 4
  
  ProductCategoryDeltaTable.alias("table") \
    .merge(transformed_df.filter('Table=="ProductCategory"').select("ProductCategory.*").alias("dataframe"),
           "table.ProductCategoryID = dataframe.ProductCategoryID and table.ParentProductCategoryID = dataframe.ParentProductCategoryID") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()  #// product_category 5  
  
  ProductDescriptionDeltaTable.alias("table") \
    .merge(transformed_df.filter('Table=="ProductDescription"').select("ProductDescription.*").alias("dataframe"),
           "table.ProductDescriptionID = dataframe.ProductDescriptionID") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()  #// product_description 6  
  
  ProductModelDeltaTable.alias("table") \
    .merge(transformed_df.filter('Table=="ProductModel"').select("ProductModel.*").alias("dataframe"),
           "table.ProductModelID = dataframe.ProductModelID") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()  #// product_model 7  
  
  ProductModelProductDescriptionDeltaTable.alias("table") \
    .merge(transformed_df.filter('Table=="ProductModelProductDescription"').select("ProductModelProductDescription.*").alias("dataframe"),
           "table.ProductModelID = dataframe.ProductModelID and table.ProductDescriptionID = dataframe.ProductDescriptionID ") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()  #// product_model_product_description 8
  
  SalesOrderDetailDeltaTable.alias("table") \
    .merge(transformed_df.filter('Table=="SalesOrderDetail"').select("SalesOrderDetail.*").alias("dataframe"),
           "table.SalesOrderDetailID = dataframe.SalesOrderDetailID") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()  #// sales_order_detail 9
  
  SalesOrderHeaderDeltaTable.alias("table") \
    .merge(transformed_df.filter('Table=="SalesOrderHeader"').select("SalesOrderHeader.*").alias("dataframe"),
           "table.SalesOrderID = dataframe.SalesOrderID") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()  #// sales_order_header 10
  
  # clear cache
  transformed_df.unpersist()
  