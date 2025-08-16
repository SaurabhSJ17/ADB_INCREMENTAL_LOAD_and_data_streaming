# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.Orders (
# MAGIC     OrderID INT,
# MAGIC     OrderDate DATE,
# MAGIC     CustomerID INT,
# MAGIC     CustomerName VARCHAR(100),
# MAGIC     CustomerEmail VARCHAR(100),
# MAGIC     ProductID INT,
# MAGIC     ProductName VARCHAR(100),
# MAGIC     ProductCategory VARCHAR(50),
# MAGIC     RegionID INT,
# MAGIC     RegionName VARCHAR(50),
# MAGIC     Country VARCHAR(50),
# MAGIC     Quantity INT,
# MAGIC     UnitPrice DECIMAL(10,2),
# MAGIC     TotalAmount DECIMAL(10,2)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sales.Orders (OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount) 
# MAGIC VALUES 
# MAGIC (1, '2024-02-01', 101, 'Alice Johnson', 'alice@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'USA', 2, 800.00, 1600.00),
# MAGIC (2, '2024-02-02', 102, 'Bob Smith', 'bob@example.com', 202, 'Smartphone', 'Electronics', 302, 'Europe', 'Germany', 1, 500.00, 500.00),
# MAGIC (3, '2024-02-03', 103, 'Charlie Brown', 'charlie@example.com', 203, 'Tablet', 'Electronics', 303, 'Asia', 'India', 3, 300.00, 900.00),
# MAGIC (4, '2024-02-04', 101, 'Alice Johnson', 'alice@example.com', 204, 'Headphones', 'Accessories', 301, 'North America', 'USA', 1, 150.00, 150.00),
# MAGIC (5, '2024-02-05', 104, 'David Lee', 'david@example.com', 205, 'Gaming Console', 'Electronics', 302, 'Europe', 'France', 1, 400.00, 400.00),
# MAGIC (6, '2024-02-06', 102, 'Bob Smith', 'bob@example.com', 206, 'Smartwatch', 'Electronics', 303, 'Asia', 'China', 2, 200.00, 400.00),
# MAGIC (7, '2024-02-07', 105, 'Eve Adams', 'eve@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'Canada', 1, 800.00, 800.00),
# MAGIC (8, '2024-02-08', 106, 'Frank Miller', 'frank@example.com', 207, 'Monitor', 'Accessories', 302, 'Europe', 'Italy', 2, 250.00, 500.00),
# MAGIC (9, '2024-02-09', 107, 'Grace White', 'grace@example.com', 208, 'Keyboard', 'Accessories', 303, 'Asia', 'Japan', 3, 100.00, 300.00),
# MAGIC (10, '2024-02-10', 104, 'David Lee', 'david@example.com', 209, 'Mouse', 'Accessories', 301, 'North America', 'USA', 1, 50.00, 50.00);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sales.Orders (OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount) 
# MAGIC VALUES 
# MAGIC (11, '2024-02-11', 101, 'Alice Johnson', 'alice@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'USA', 2, 800.00, 1600.00),
# MAGIC (12, '2024-02-12', 102, 'Bob Smith', 'bob@example.com', 202, 'Smartphone', 'Electronics', 302, 'Europe', 'Germany', 1, 500.00, 500.00),
# MAGIC (13, '2024-02-13', 103, 'Charlie Brown', 'charlie@example.com', 203, 'Tablet', 'Electronics', 303, 'Asia', 'India', 3, 300.00, 900.00),
# MAGIC (14, '2024-02-14', 101, 'Alice Johnson', 'alice@example.com', 204, 'Headphones', 'Accessories', 301, 'North America', 'USA', 1, 150.00, 150.00);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS  sales_wh;

# COMMAND ----------

# MAGIC %sql
# MAGIC create database IF NOT EXISTS staging;

# COMMAND ----------

# MAGIC %md
# MAGIC ##staging 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE staging.sales_stg
# MAGIC AS
# MAGIC SELECT * FROM sales.orders
# MAGIC WHERE OrderDate > '2024-02-10';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM staging.sales_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW hive_metastore.staging.trans_sales AS
# MAGIC SELECT *
# MAGIC FROM hive_metastore.staging.sales_stg
# MAGIC WHERE Quantity IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.staging.trans_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ##WAREHOUSE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sales_wh.core_table
# MAGIC AS
# MAGIC SELECT * FROM hive_metastore.staging.trans_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO hive_metastore.sales_wh.core_table AS tgt
# MAGIC USING hive_metastore.staging.trans_sales AS src
# MAGIC ON tgt.OrderID = src.OrderID
# MAGIC WHEN MATCHED THEN
# MAGIC   DELETE
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.sales_wh.core_table;
