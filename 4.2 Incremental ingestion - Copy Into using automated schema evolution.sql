-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####Setup

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS dev;
CREATE DATABASE IF NOT EXISTS dev.demo_db02;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. Verify you can access the invoices directory

-- COMMAND ----------

-- MAGIC %fs ls abfss://invocie-data@adlssmiloeabronzedev001.dfs.core.windows.net

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Create a schemaless delta table to ingest invoices data

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dev.demo_db02.invoices_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Describe the shemaless table

-- COMMAND ----------

DESCRIBE EXTENDED dev.demo_db02.invoices_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Ingest data into invoices_raw table using copy into command

-- COMMAND ----------

COPY INTO dev.demo_db02.invoices_raw
FROM "abfss://invocie-data@adlssmiloeabronzedev001.dfs.core.windows.net"
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'timestampFormat' = 'd-M-y H.m', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Check the records after ingestion

-- COMMAND ----------

SELECT * FROM dev.demo_db02.invoices_raw limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Describe the table after ingestion

-- COMMAND ----------

DESCRIBE dev.demo_db02.invoices_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####6. Ingest some more data into the invoices directory with an additional column

-- COMMAND ----------

-- MAGIC %fs cp /mnt/files/dataset_ch8/invoices_2021.csv /mnt/files/dataset_ch8/invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####7. Ingest data into invoices_raw table again

-- COMMAND ----------

COPY INTO dev.demo_db02.invoices_raw
FROM "abfss://invocie-data@adlssmiloeabronzedev001.dfs.core.windows.net"
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'timestampFormat' = 'd-M-y H.m', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####8. Check the record count and records after ingestion

-- COMMAND ----------

SELECT count(*) FROM dev.demo_db02.invoices_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####9. Describe the invoices_raw table after ingestion

-- COMMAND ----------

DESCRIBE dev.demo_db02.invoices_raw
