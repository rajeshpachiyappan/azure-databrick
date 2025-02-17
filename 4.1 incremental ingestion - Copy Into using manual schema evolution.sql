-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS dev;
CREATE DATABASE IF NOT EXISTS dev.demo_db01;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. Verify you can access the invoices directory

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create external location pointing to invocie container and then list invoice container as below

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC %fs ls abfss://invocie-data@adlssmiloeabronzedev001.dfs.core.windows.net

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Create a delta table to ingest invoices data

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dev.demo_db01.invoices_raw(
  InvoiceNo string,
  StockCode string,
  Description string,
  Quantity int,
  InvoiceDate timestamp,
  UnitPrice double,
  CustomerID string)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Ingest data into invoices_raw table using copy into command

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######3.1 Ingest using copy into command

-- COMMAND ----------

COPY INTO dev.demo_db01.invoices_raw
FROM (SELECT InvoiceNo::string, StockCode::string, Description::string, Quantity::int,
        to_timestamp(InvoiceDate,'d-M-y H.m') InvoiceDate, UnitPrice::double, CustomerID::string
      FROM "abfss://invocie-data@adlssmiloeabronzedev001.dfs.core.windows.net")
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######3.2 Check the records after ingestion

-- COMMAND ----------

SELECT * FROM dev.demo_db01.invoices_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######3.3 COPY INTO is idempotent

-- COMMAND ----------

COPY INTO dev.demo_db01.invoices_raw
FROM (SELECT InvoiceNo::string, StockCode::string, Description::string, Quantity::int,
        to_timestamp(InvoiceDate,'d-M-y H.m') InvoiceDate, UnitPrice::double, CustomerID::string
      FROM "abfss://invocie-data@adlssmiloeabronzedev001.dfs.core.windows.net")
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######3.4 Check the records after ingestion

-- COMMAND ----------

SELECT count(*) FROM dev.demo_db01.invoices_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Collect more data into the invoices directory which comes with an additional column

-- COMMAND ----------

-- MAGIC %fs cp /mnt/files/dataset_ch8/invoices_2021.csv /mnt/files/dataset_ch8/invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Your ingestion code will not break but silently ignore the additional column

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######5.1 Alter table to mnaully accomodate the additional field

-- COMMAND ----------

ALTER TABLE dev.demo_db01.invoices_raw ADD COLUMNS (Country string)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######5.2 Modify your ingestion code to manually accomodate the additional field

-- COMMAND ----------

COPY INTO dev.demo_db01.invoices_raw
FROM (SELECT InvoiceNo::string, StockCode::string, Description::string, Quantity::int,
        to_timestamp(InvoiceDate,'d-M-y H.m') InvoiceDate, UnitPrice::double, CustomerID::string, Country::string
      FROM "abfss://invocie-data@adlssmiloeabronzedev001.dfs.core.windows.net")
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'mergeSchema' = 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######5.3 Check the records after ingestion

-- COMMAND ----------

SELECT count(*) FROM dev.demo_db01.invoices_raw
