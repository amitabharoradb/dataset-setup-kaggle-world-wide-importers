-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Needs work

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## _NOTE: Please change the catalog and schema names in the cell below_

-- COMMAND ----------

USE CATALOG amitabh_arora_catalog;
use SCHEMA kaggle_world_wide_importers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Purchase orders (purchasing_purchaseorders)

-- COMMAND ----------

select * from purchasing_purchaseorders
ORDER BY CAST(SupplierID AS INT) ASC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Number of suppliers

-- COMMAND ----------

SELECT DISTINCT SupplierID 
FROM purchasing_purchaseorders 
ORDER BY CAST(SupplierID AS INT) ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Suppliers with most POs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
