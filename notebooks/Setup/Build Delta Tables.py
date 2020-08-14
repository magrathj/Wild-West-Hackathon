# Databricks notebook source
# MAGIC %md 
# MAGIC ## Create Your Team's Database
# MAGIC - No Spaces
# MAGIC - Recommend Pascal Casing (LikeThisForExample)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS StructuredStreaming

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create SalesOrderDetail Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE StructuredStreaming.SalesOrderDetail (
# MAGIC   LineTotal DOUBLE,
# MAGIC   ModifiedDate STRING,
# MAGIC   OrderQty LONG,
# MAGIC   ProductID LONG,
# MAGIC   SalesOrderDetailID LONG,
# MAGIC   SalesOrderID LONG,
# MAGIC   UnitPrice DOUBLE,
# MAGIC   UnitPriceDiscount DOUBLE,
# MAGIC   rowguid STRING
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create SalesOrderHeader Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE StructuredStreaming.SalesOrderHeader (
# MAGIC   SalesOrderID int,
# MAGIC   RevisionNumber int,
# MAGIC   OrderDate timestamp,
# MAGIC   DueDate timestamp,
# MAGIC   ShipDate timestamp,
# MAGIC   Status int,
# MAGIC   OnlineOrderFlag boolean,
# MAGIC   SalesOrderNumber string,
# MAGIC   PurchaseOrderNumber string,
# MAGIC   AccountNumber string,
# MAGIC   CustomerID int,
# MAGIC   ShipToAddressID int,
# MAGIC   BillToAddressID int,
# MAGIC   ShipMethod string,
# MAGIC   CreditCardApprovalCode string,
# MAGIC   SubTotal decimal(19,4),
# MAGIC   TaxAmt decimal(19,4),
# MAGIC   Freight decimal(19,4),
# MAGIC   TotalDue decimal(19,4),
# MAGIC   Comment string,
# MAGIC   rowguid string,
# MAGIC   ModifiedDate timestamp
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %md ## Create Customer Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE StructuredStreaming.Customer(
# MAGIC   CustomerID int,
# MAGIC   NameStyle boolean,
# MAGIC   Title string,
# MAGIC   FirstName string,
# MAGIC   MiddleName string,
# MAGIC   LastName string,
# MAGIC   Suffix string,
# MAGIC   CompanyName string,
# MAGIC   SalesPerson string,
# MAGIC   EmailAddress string,
# MAGIC   Phone string,
# MAGIC   PasswordHash string,
# MAGIC   PasswordSalt string,
# MAGIC   rowguid string,
# MAGIC   ModifiedDate timestamp
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %md ## Create Address Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE StructuredStreaming.Address (
# MAGIC  AddressID LONG, 
# MAGIC  AddressLine1 STRING,
# MAGIC  AddressLine2 STRING,
# MAGIC  City STRING,
# MAGIC  StateProvince STRING,
# MAGIC  CountryRegion STRING,
# MAGIC  PostalCode STRING,
# MAGIC  rowguid STRING,
# MAGIC  ModifiedDate TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %md ## Create CustomerAddress Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE StructuredStreaming.CustomerAddress (
# MAGIC   CustomerID int,
# MAGIC   AddressID int,
# MAGIC   AddressType string,
# MAGIC   rowguid string,
# MAGIC   ModifiedDate timestamp
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %md ## Create Product Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE StructuredStreaming.Product (
# MAGIC   ProductID int,
# MAGIC   Name string,
# MAGIC   ProductNumber string,
# MAGIC   Color string,
# MAGIC   StandardCost decimal(19,4),
# MAGIC   ListPrice decimal(19,4),
# MAGIC   Size string,
# MAGIC   Weight decimal(8,2),
# MAGIC   ProductCategoryID int,
# MAGIC   ProductModelID int,
# MAGIC   SellStartDate timestamp,
# MAGIC   SellEndDate timestamp,
# MAGIC   DiscontinuedDate timestamp,
# MAGIC   ThumbNailPhoto binary,
# MAGIC   ThumbnailPhotoFileName string,
# MAGIC   rowguid string,
# MAGIC   ModifiedDate timestamp
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %md ## Create ProductDescription Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE StructuredStreaming.ProductDescription (
# MAGIC   ProductDescriptionID int,
# MAGIC   Description string,
# MAGIC   rowguid string,
# MAGIC   ModifiedDate timestamp
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %md ## Create ProductCategory Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE StructuredStreaming.ProductCategory (
# MAGIC   ProductCategoryID int,
# MAGIC   ParentProductCategoryID int,
# MAGIC   Name string,
# MAGIC   rowguid string,
# MAGIC   ModifiedDate timestamp
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %md ## Create ProductModel Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE StructuredStreaming.ProductModel (
# MAGIC   ProductModelID int,
# MAGIC   Name string,
# MAGIC   CatalogDescription string,
# MAGIC   rowguid string,
# MAGIC   ModifiedDate timestamp
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %md ## Create ProductModelProductDescription Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE StructuredStreaming.ProductModelProductDescription (
# MAGIC   ProductModelID int,
# MAGIC   ProductDescriptionID int,
# MAGIC   Culture string,
# MAGIC   rowguid string,
# MAGIC   ModifiedDate timestamp
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.notebook.exit("Success")