-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Needs work

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Parameterize catalog and schema

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # define default parameters
-- MAGIC # notebook user
-- MAGIC notebook_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
-- MAGIC notebook_user = notebook_user.split('@')[0].replace('.', '_')
-- MAGIC
-- MAGIC # default catalog/schema
-- MAGIC default_catalog = notebook_user + "_catalog"
-- MAGIC default_schema = "kaggle_world_wide_importers"
-- MAGIC
-- MAGIC # Parameters
-- MAGIC dbutils.widgets.text("catalog_name", default_catalog)
-- MAGIC dbutils.widgets.text("schema_name", default_schema)
-- MAGIC
-- MAGIC catalog_name = dbutils.widgets.get("catalog_name")
-- MAGIC schema_name = dbutils.widgets.get("schema_name")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"USE CATALOG {catalog_name}")
-- MAGIC spark.sql(f"USE SCHEMA {schema_name}")

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
