# Databricks notebook source
# MAGIC %md
# MAGIC ## Define parameters to the notebook

# COMMAND ----------

# MAGIC %run ./_resources

# COMMAND ----------

# Parameters
dbutils.widgets.text("catalog_name", default_catalog)
dbutils.widgets.text("schema_name", default_schema)
dbutils.widgets.text("secrets_scope", default_scope)

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
secrets_scope = dbutils.widgets.get("secrets_scope")

# COMMAND ----------

# Parameters
dbutils.widgets.text("catalog_name", {default_catalog})
dbutils.widgets.text("schema_name", {default_schema})
dbutils.widgets.text("secrets_scope", {default_scope})

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
secrets_scope = dbutils.widgets.get("secrets_scope")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load kaggle credentials from Databricks Secrets
# MAGIC Please read the Readme.md if kaggle credentials does not exist or are expired

# COMMAND ----------

kaggle_username = dbutils.secrets.get(scope = secrets_scope, key = "kaggle_username")
kaggle_key = dbutils.secrets.get(scope = secrets_scope, key = "kaggle_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Kaggle Supply Chain dataset into your catalog and your schema
# MAGIC
# MAGIC **NOTE**: This notbook deletes the existing schema and re-creates to ensure clean data

# COMMAND ----------

# DBTITLE 1,kaggle dataset specifications
kaggle_url = "https://www.kaggle.com/datasets/pauloviniciusornelas/wwimporters"
kaggle_dataset_user = 'pauloviniciusornelas'
kaggle_dataset_name = 'wwimporters'
kaggle_dataset_id = f"{kaggle_dataset_user}/{kaggle_dataset_name}"

# folder location where the data will be downloaded and unzipped
dataset_downloads_volume_name = "downloads"
dataset_download_location = f"/Volumes/{catalog_name}/{schema_name}/{dataset_downloads_volume_name}"
print(dataset_download_location)

# COMMAND ----------

# DBTITLE 1,Load Kaggle Secrets file
import os

# secrets_json = None
# with open(kaggle_secrets_file) as json_file:
#     secrets_json = json.load(json_file)

# Environment variables for authentication
os.environ['KAGGLE_USERNAME'] = kaggle_username
os.environ['KAGGLE_KEY'] = kaggle_key

# COMMAND ----------

# DBTITLE 1,re-crrate schema and download location
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE")
spark.sql(f"CREATE SCHEMA {catalog_name}.{schema_name}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{dataset_downloads_volume_name}")

# COMMAND ----------

# DBTITLE 1,set use catalog and schema
# Set the catalog and schema to use
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

!pip install kaggle

# COMMAND ----------

# DBTITLE 1,download the dataset file from kaggle
# Code references from this nice youtube video: https://www.youtube.com/watch?v=hzcV0hDkfzs

import kaggle
kaggle.api.authenticate()

kaggle.api.dataset_download_files(kaggle_dataset_id, path=dataset_download_location, unzip=True)
kaggle.api.dataset_metadata(kaggle_dataset_id, path=dataset_download_location)


# COMMAND ----------

# DBTITLE 1,function to get a list of CSV files recursively in a folder
def get_csv_files(directory_path):
  """recursively list path of all csv files in path directory """
  csv_files = []
  files_to_treat = dbutils.fs.ls(directory_path)
  while files_to_treat:
    path = files_to_treat.pop(0).path
    if path.endswith('/'):
      files_to_treat += dbutils.fs.ls(path)
    elif path.endswith('.csv'):
      csv_files.append(path)
  return csv_files

# COMMAND ----------

# DBTITLE 1,get an array of CSV files
csv_files = get_csv_files(dataset_download_location)
print(len(csv_files))

# COMMAND ----------

# DBTITLE 1,define a function to get table name from file name
from pathlib import Path

def file_to_table_name(file_name) -> str:
  basename = Path(file_name).stem
  table_name = basename.replace(".", "_")
  return table_name


# COMMAND ----------

# DBTITLE 1,function to create the table given the CSV file
def load_csv_file(csv_file_path):
  table_name = file_to_table_name(csv_file_path)

  df = spark.read.format("csv").option("header", "true").option("delimiter", ";").load(csv_file_path)
  df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")
  print(f"created table: {table_name}")


# COMMAND ----------

# DBTITLE 1,loop through each csv file and create table
for csv_file in csv_files:
  load_csv_file(csv_file)

# COMMAND ----------

# DBTITLE 1,delete Kaggle download volume
spark.sql(f"DROP VOLUME IF EXISTS {catalog_name}.{schema_name}.{dataset_downloads_volume_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create function inspired by an external function available at DBR level

# COMMAND ----------

# DBTITLE 1,day_interval
# This function is a clone of main.default.day_interval()
# Just copied here, but not using it anymore.
sql_function = f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.day_interval(end_date DATE, start_date DATE)
RETURNS INT
LANGUAGE SQL
RETURN CEIL(ABS(UNIX_TIMESTAMP(end_date)-UNIX_TIMESTAMP(start_date)) / (60 * 60 * 24))
"""

spark.sql(sql_function)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create VIEWS the way Robert Mosley did

# COMMAND ----------

# DBTITLE 1,vw_purchasing_purchaseorders
select_statement = f"""
select {catalog_name}.{schema_name}.day_interval(expecteddeliverydate, orderdate) as ExpectedLeadDays, *
from {catalog_name}.{schema_name}.purchasing_purchaseorders
"""

view_name = "vw_purchasing_purchaseorders"
spark.sql(f"CREATE OR REPLACE VIEW {catalog_name}.{schema_name}.{view_name} AS {select_statement}")

# COMMAND ----------

# DBTITLE 1,vw_purchasing_suppliers
select_statement = f"""
with leaddays as (
  select
    SupplierID,
    round(avg(ExpectedLeadDays), 0) as AverageLeadDays
  from
    {catalog_name}.{schema_name}.vw_purchasing_purchaseorders
  group by
    all
)
select
  s.*,
  l.AverageLeadDays
from
  {catalog_name}.{schema_name}.purchasing_suppliers s
  left outer join leaddays l on s.SupplierID = l.SupplierID
"""

view_name = "vw_purchasing_suppliers"
spark.sql(f"CREATE OR REPLACE VIEW {catalog_name}.{schema_name}.{view_name} AS {select_statement}")

# COMMAND ----------

# DBTITLE 1,vw_warehouse_stockitemtransaction
select_statement = f"""
select 
  t.StockItemTransactionID,
  t.StockItemID,
  si.StockItemName,
  t.TransactionTypeID,
  tt.TransactionTypeName,
  t.CustomerID,
  c.CustomerName,
  t.InvoiceID,
  t.SupplierID,
  s.SupplierName,
  t.PurchaseOrderID,
  t.TransactionOccurredWhen,
  t.Quantity
from 
  {catalog_name}.{schema_name}.warehouse_stockitemtransactions t
left join 
  {catalog_name}.{schema_name}.warehouse_stockitems si on t.StockItemID = si.StockItemID
left join 
  {catalog_name}.{schema_name}.application_transactiontypes tt on t.TransactionTypeID = tt.TransactionTypeID
left join 
  {catalog_name}.{schema_name}.sales_customers c on t.CustomerID = c.CustomerID
left join 
  {catalog_name}.{schema_name}.sales_invoices i on t.InvoiceID = i.InvoiceID
left join 
  {catalog_name}.{schema_name}.purchasing_suppliers s on t.SupplierID = s.SupplierID
left join 
  {catalog_name}.{schema_name}.purchasing_purchaseorders po on t.PurchaseOrderID = po.PurchaseOrderID
"""

view_name = "vw_warehouse_stockitemtransaction"
spark.sql(f"CREATE OR REPLACE VIEW {catalog_name}.{schema_name}.{view_name} AS {select_statement}")

# COMMAND ----------

# DBTITLE 1,vw_purchasing_unfilled
select_statement = f"""
select l.PurchaseOrderID, l.PurchaseOrderLineID, l.StockItemID, int(l.OrderedOuters) as Quantity, date(po.ExpectedDeliveryDate)
from {catalog_name}.{schema_name}.purchasing_purchaseorderlines l
  inner join {catalog_name}.{schema_name}.purchasing_purchaseorders po on l.PurchaseOrderID = po.PurchaseOrderID
where l.LastReceiptDate = 'NULL'
"""

view_name = "vw_purchasing_unfilled"
spark.sql(f"CREATE OR REPLACE VIEW {catalog_name}.{schema_name}.{view_name} AS {select_statement}")

# COMMAND ----------

# DBTITLE 1,vw_sales_unfilled
select_statement = f"""
select l.OrderID, l.OrderLineID, l.StockItemID, -1 * int(l.Quantity) Quantity, to_date(so.ExpectedDeliveryDate, 'dd/MM/yyyy') as InventoryImpactDate
from {catalog_name}.{schema_name}.sales_orderlines l
  inner join {catalog_name}.{schema_name}.sales_orders so on l.OrderID = so.orderid 
where l.PickingCompletedWhen = 'NULL'
  and to_date(so.ExpectedDeliveryDate, 'dd/MM/yyyy') >= '2016-06-01'
"""

view_name = "vw_sales_unfilled"
spark.sql(f"CREATE OR REPLACE VIEW {catalog_name}.{schema_name}.{view_name} AS {select_statement}")

# COMMAND ----------

# DBTITLE 1,vw_stock_transactions
select_statement = f"""
with transactions as (
select 'SALES' as OrderType, * from {catalog_name}.{schema_name}.vw_sales_unfilled
union all
select 'PURCHASE' as OrderType, * from {catalog_name}.{schema_name}.vw_purchasing_unfilled where PurchaseOrderID <> '2074'
)
, full as (
select * from transactions
union all
select 'ON-HAND', Null, null, StockItemID, QuantityOnHand, date('2016-05-31')  from {catalog_name}.{schema_name}.warehouse_stockitemholdings where QuantityOnHand < 1000 or StockItemID in (select StockItemID from transactions)
order by 4, 6
)
, full_qty as (
select *
  , coalesce(lag(quantity) over(partition by StockItemID order by InventoryImpactDate), 0) + quantity as ExpectedQuantityOnHand
from full
)
select f.*  
  , s.StockItemName
  , s.SupplierID as Stock_SupplierID
  , s.LeadTimeDays as Stock_LeadTimeDays
from full_qty f
  inner join {catalog_name}.{schema_name}.warehouse_stockitems s on f.StockItemID = s.StockItemID
"""

view_name = "vw_stock_transactions"
spark.sql(f"CREATE OR REPLACE VIEW {catalog_name}.{schema_name}.{view_name} AS {select_statement}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create FUNCTIONS the way Robert Mosley did

# COMMAND ----------

# DBTITLE 1,find_stockitemid
# Functon copied from robert_mosley.supply_chain schema
sql_function = f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.find_stockitemid(stock_name_fragment string
  comment 'Search for a StockItemID by submitting a fragment of the item name. For best results, only submit a single word - it has to match exactly (case insensitive)')
RETURNS TABLE (StockItemID string, StockItemName string)
LANGUAGE SQL
RETURN
  (SELECT StockItemID, StockItemName
  from {catalog_name}.{schema_name}.warehouse_stockitems 
  where upper(StockItemName) like '%'||upper(stock_name_fragment)||'%'
    and stockitemid in (select StockItemID from {catalog_name}.{schema_name}.vw_stock_transactions)
  order by StockItemID)
"""

spark.sql(sql_function)

# COMMAND ----------

# DBTITLE 1,test: find_stockitemid('brown eggs')
# Test find_stock_item_id
result_df = spark.sql(f"""
  SELECT * 
  FROM {catalog_name}.{schema_name}.find_stockitemid('brown eggs')
""")
display(result_df)

# COMMAND ----------

# DBTITLE 1,get_stock_inventory
# Functon copied from robert_mosley.supply_chain schema
sql_function = f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.get_stock_inventory(ItemID string comment 'Returns inventory transactions for provided ItemID = StockItemID. Results will include inventory onhand and expected sales orders / purchase orders along with their impacts to the quantity on hand.')
RETURNS TABLE (OrderType STRING, OrderID STRING, OrderLineID STRING, StockItemID STRING, Quantity INT, InventoryImpactDate DATE, ExpectedQuantityOnHand INT, StockItemName STRING, Stock_SupplierID STRING, Stock_LeadTimeDays INT)
LANGUAGE SQL
RETURN
  (SELECT t.*
from {catalog_name}.{schema_name}.vw_stock_transactions t
where t.StockItemID = ItemID
order by t.InventoryImpactDate asc)
"""

spark.sql(sql_function)

# COMMAND ----------

# DBTITLE 1,stock_at_risk
# Functon copied from robert_mosley.supply_chain schema
sql_function = f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.stock_at_risk()
RETURNS TABLE (OrderType STRING, OrderID STRING, OrderLineID STRING, StockItemID STRING, Quantity INT, InventoryImpactDate DATE, ExpectedQuantityOnHand INT, StockItemName STRING, Stock_SupplierID STRING, Stock_LeadTimeDays INT)
LANGUAGE SQL
RETURN
  (SELECT t.*
    from robert_mosley.supply_chain.vw_stock_transactions t
    where t.stockitemid in (select stockitemid from {catalog_name}.{schema_name}.vw_stock_transactions t where t.ExpectedQuantityOnHand < 0)
    order by t.stockitemid, t.InventoryImpactDate asc)
"""

spark.sql(sql_function)
