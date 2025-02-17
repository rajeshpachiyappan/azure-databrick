# Databricks notebook source
# MAGIC %md
# MAGIC #####Setup

# COMMAND ----------

base_dir = "abfss://invocie-data@adlssmiloeabronzedev001.dfs.core.windows.net"
spark.sql("CREATE CATALOG IF NOT EXISTS dev")
spark.sql("CREATE DATABASE IF NOT EXISTS dev.demo_db05")

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Verify you can access the invoices directory

# COMMAND ----------

# MAGIC %fs ls abfss://invocie-data@adlssmiloeabronzedev001.dfs.core.windows.net

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Ingest data into invoices_raw table using spark streaming api

# COMMAND ----------

def ingest():
    invoice_schema = """InvoiceNo int, StockCode string, Description string, Quantity int, 
                    InvoiceDate timestamp, UnitPrice double, CustomerID int"""
    source_df = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", "csv")  
                      .option("header", "true") 
                      .option("timestampFormat","d-M-y H.m")                  
                      .option("cloudFiles.schemaLocation", f"{base_dir}/chekpoint/invoices_schema")
                      .option("cloudFiles.inferColumnTypes", "true")
                      .schema(invoice_schema)
                      .load(f"{base_dir}")
    )

    write_query = (source_df.writeStream
                          .format("delta")
                          .option("checkpointLocation", f"{base_dir}/chekpoint/invoices")
                          .option("mergeSchema", "true")
                          .outputMode("append")                          
                          .trigger(availableNow=True)
                          .toTable("dev.demo_db05.invoices_raw01")
    )

ingest()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.demo_db05.invoices_raw01 limit 5

# COMMAND ----------

def ingest():
  source_df = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", "csv")  
                      .option("header", "true") 
                      .option("timestampFormat","d-M-y H.m")                  
                      .option("cloudFiles.schemaLocation", f"{base_dir}/chekpoint/invoices_schema")
                      .option("cloudFiles.inferColumnTypes", "true")
                      .option("cloudFiles.schemaHints", "InvoiceNo string, CustomerID string")
                      .load(f"{base_dir}/")
  )

  write_query = (source_df.writeStream
                          .format("delta")
                          .option("checkpointLocation", f"{base_dir}/chekpoint/invoices")
                          .option("mergeSchema", "true")
                          .outputMode("append")                          
                          .trigger(availableNow = True)
                          .toTable("dev.demo_db05.invoices_raw03")
  )

ingest() 

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Check the records after ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM dev.demo_db05.invoices_raw01 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE dev.demo_db05.invoices_raw01 

# COMMAND ----------

# MAGIC %sql
# MAGIC desc dev.demo_db05.invoices_raw02

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Ingest some more data into the invoices directory which comes with an additional column

# COMMAND ----------

# MAGIC %fs cp /mnt/files/dataset_ch8/invoices_2021.csv /mnt/files/dataset_ch8/invoices/

# COMMAND ----------

# MAGIC %md
# MAGIC #####5. Ingest with a retry

# COMMAND ----------

ingest()

# COMMAND ----------

# MAGIC %md
# MAGIC #####6. Check the data 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.demo_db.invoices_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE dev.demo_db.invoices_raw

# COMMAND ----------

# MAGIC %md
# MAGIC #####7. Ingest some more records with potential bad records

# COMMAND ----------

# MAGIC %fs cp /mnt/files/dataset_ch8/invoices_2022.csv /mnt/files/dataset_ch8/invoices/

# COMMAND ----------

ingest()

# COMMAND ----------

# MAGIC %md
# MAGIC #####8. Check the rescued data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.demo_db.invoices_raw where _rescued_data is not null
