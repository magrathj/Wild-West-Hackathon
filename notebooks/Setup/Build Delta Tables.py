# Databricks notebook source
# MAGIC %md 
# MAGIC ## Create Your Team's Database
# MAGIC - No Spaces
# MAGIC - Recommend Pascal Casing (LikeThisForExample)

# COMMAND ----------

# MAGIC %run "./../Configuration/App Config"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS StructuredStreaming

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create SalesOrderDetail Delta Table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE StructuredStreaming.SalesOrderDetail (
  LineTotal decimal(38,6),
  ModifiedDate STRING,
  OrderQty LONG,
  ProductID LONG,
  SalesOrderDetailID LONG,
  SalesOrderID LONG,
  UnitPrice DOUBLE,
  UnitPriceDiscount DOUBLE,
  rowguid STRING
)
USING DELTA LOCATION '{location}'  
""".format(location=SALES_ORDER_DETAIL_DELTA))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create SalesOrderHeader Delta Table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE StructuredStreaming.SalesOrderHeader (
  SalesOrderID int,
  RevisionNumber int,
  OrderDate timestamp,
  DueDate timestamp,
  ShipDate timestamp,
  Status int,
  OnlineOrderFlag boolean,
  SalesOrderNumber string,
  PurchaseOrderNumber string,
  AccountNumber string,
  CustomerID int,
  ShipToAddressID int,
  BillToAddressID int,
  ShipMethod string,
  CreditCardApprovalCode string,
  SubTotal decimal(19,4),
  TaxAmt decimal(19,4),
  Freight decimal(19,4),
  TotalDue decimal(19,4),
  Comment string,
  rowguid string,
  ModifiedDate timestamp
)
USING DELTA LOCATION '{location}' 
""".format(location=SALES_ORDER_HEADER_DELTA))

# COMMAND ----------

# MAGIC %md ## Create Customer Delta Table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE StructuredStreaming.Customer(
  CustomerID int,
  NameStyle boolean,
  Title string,
  FirstName string,
  MiddleName string,
  LastName string,
  Suffix string,
  CompanyName string,
  SalesPerson string,
  EmailAddress string,
  Phone string,
  PasswordHash string,
  PasswordSalt string,
  rowguid string,
  ModifiedDate timestamp
)
USING DELTA LOCATION '{location}' 
""".format(location=CUSTOMER_DELTA))

# COMMAND ----------

# MAGIC %md ## Create Address Delta Table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE StructuredStreaming.Address (
 AddressID INT, 
 AddressLine1 STRING,
 AddressLine2 STRING,
 City STRING,
 StateProvince STRING,
 CountryRegion STRING,
 PostalCode STRING,
 rowguid STRING,
 ModifiedDate TIMESTAMP
)
USING DELTA LOCATION '{location}' 
""".format(location=ADDRESS_DELTA))

# COMMAND ----------

# MAGIC %md ## Create CustomerAddress Delta Table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE StructuredStreaming.CustomerAddress (
  CustomerID int,
  AddressID int,
  AddressType string,
  rowguid string,
  ModifiedDate timestamp
)
USING DELTA LOCATION '{location}' 
""".format(location=CUSTOMER_ADDRESS_DELTA))

# COMMAND ----------

# MAGIC %md ## Create Product Delta Table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE StructuredStreaming.Product (
  ProductID int,
  Name string,
  ProductNumber string,
  Color string,
  StandardCost decimal(19,4),
  ListPrice decimal(19,4),
  Size string,
  Weight decimal(8,2),
  ProductCategoryID int,
  ProductModelID int,
  SellStartDate timestamp,
  SellEndDate timestamp,
  DiscontinuedDate timestamp,
  ThumbNailPhoto binary,
  ThumbnailPhotoFileName string,
  rowguid string,
  ModifiedDate timestamp
)
USING DELTA LOCATION '{location}' 
""".format(location=PRODUCT_DELTA))

# COMMAND ----------

# MAGIC %md ## Create ProductDescription Delta Table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE StructuredStreaming.ProductDescription (
  ProductDescriptionID int,
  Description string,
  rowguid string,
  ModifiedDate timestamp
)
USING DELTA LOCATION '{location}' 
""".format(location=PRODUCT_DESCRIPTION_DELTA))

# COMMAND ----------

# MAGIC %md ## Create ProductCategory Delta Table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE StructuredStreaming.ProductCategory (
  ProductCategoryID int,
  ParentProductCategoryID int,
  Name string,
  rowguid string,
  ModifiedDate timestamp
)
USING DELTA LOCATION '{location}' 
""".format(location=PRODUCT_CATEGORGY_DELTA))

# COMMAND ----------

# MAGIC %md ## Create ProductModel Delta Table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE StructuredStreaming.ProductModel (
  ProductModelID int,
  Name string,
  CatalogDescription string,
  rowguid string,
  ModifiedDate timestamp
)
USING DELTA LOCATION '{location}' 
""".format(location=PRODUCT_MODEL_DELTA)) 

# COMMAND ----------

# MAGIC %md ## Create ProductModelProductDescription Delta Table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE StructuredStreaming.ProductModelProductDescription (
  ProductModelID int,
  ProductDescriptionID int,
  Culture string,
  rowguid string,
  ModifiedDate timestamp
)
USING DELTA LOCATION '{location}' 
""".format(location=PRODUCT_MODEL_PRODUCT_DESCRIPTION_DELTA))

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.notebook.exit("Success")