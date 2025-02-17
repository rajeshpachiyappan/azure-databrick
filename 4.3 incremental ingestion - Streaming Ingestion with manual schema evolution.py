# Databricks notebook source
base_dir = "abfss://invocie-data@adlssmiloeabronzedev001.dfs.core.windows.net"
spark.sql("CREATE CATALOG IF NOT EXISTS dev")
spark.sql("CREATE DATABASE IF NOT EXISTS dev.demo_db03")

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Verify you can access the invoices directory

# COMMAND ----------

# MAGIC %fs ls abfss://invocie-data@adlssmiloeabronzedev001.dfs.core.windows.net

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Create a delta table to ingest invoices data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.demo_db03.invoices_raw(
# MAGIC   InvoiceNo int,
# MAGIC   StockCode string,
# MAGIC   Description string,
# MAGIC   Quantity int,
# MAGIC   InvoiceDate timestamp,
# MAGIC   UnitPrice double,
# MAGIC   CustomerID int)

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Ingest data into invoices_raw table using spark streaming api

# COMMAND ----------

def ingest():
  invoice_schema = """InvoiceNo int, StockCode string, Description string, Quantity int, 
                    InvoiceDate timestamp, UnitPrice double, CustomerID int"""
                    
  source_df = (spark.readStream
                      .format("csv")
                      .option("header", "true")
                      .schema(invoice_schema)
                      .load(f"{base_dir}/")
  )

  write_query = (source_df.writeStream
                          .format("delta")
                          .option("checkpointLocation", f"{base_dir}/chekpoint/invoices")
                          .outputMode("append")
                          .trigger(availableNow = True)
                          .toTable("dev.demo_db03.invoices_raw")
  )

ingest()

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Check the records after ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM dev.demo_db03.invoices_raw

# COMMAND ----------

# MAGIC %md
# MAGIC #####6. Ingest some more data into the invoices directory which comes with an additional column

# COMMAND ----------

# MAGIC %fs cp /mnt/files/dataset_ch8/invoices_2021.csv /mnt/files/dataset_ch8/invoices

# COMMAND ----------

ingest()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM dev.demo_db03.invoices_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.demo_db03.invoices_raw limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC #####7. Your ingestion code will not break but silently ignore the additional column

# COMMAND ----------

# MAGIC %md
# MAGIC ######7.1 Alter table to evolve the schema

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dev.demo_db03.invoices_raw ADD COLUMNS (Country string)

# COMMAND ----------

# MAGIC %md
# MAGIC ######7.2 Modify streaming ingestion to accomodate shcema changes

# COMMAND ----------

def ingest():
  invoice_schema = """InvoiceNo int, StockCode string, Description string, Quantity int, 
                    InvoiceDate timestamp, UnitPrice double, CustomerID int, Country string"""
  source_df = (spark.readStream
                      .format("csv")
                      .option("header", "true")
                      .schema(invoice_schema)
                      .load(f"{base_dir}/")
  )

  write_query = (source_df.writeStream
                          .format("delta")
                          .option("checkpointLocation", f"{base_dir}/chekpoint/invoices")
                          .outputMode("append")
                          .trigger(availableNow = True)
                          .toTable("dev.demo_db03.invoices_raw")
  )

ingest()  

# COMMAND ----------

# MAGIC %md
# MAGIC #####9. Check the data 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM dev.demo_db03.invoices_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.demo_db01.invoices_raw limit 5
