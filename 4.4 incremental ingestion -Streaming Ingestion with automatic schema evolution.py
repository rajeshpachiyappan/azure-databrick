# Databricks notebook source
# MAGIC %md
# MAGIC #####Cleanup previous runs

# COMMAND ----------

# MAGIC %md
# MAGIC #####Setup

# COMMAND ----------

base_dir = "abfss://invocie-data@adlssmiloeabronzedev001.dfs.core.windows.net"
spark.sql("CREATE CATALOG IF NOT EXISTS dev")
spark.sql("CREATE DATABASE IF NOT EXISTS dev.demo_db04")

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Verify you can access the invoices directory

# COMMAND ----------

# MAGIC %fs ls abfss://invocie-data@adlssmiloeabronzedev001.dfs.core.windows.net

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Ingest data into invoices_raw table using spark streaming api

# COMMAND ----------

def ingest():
  spark.conf.set("spark.sql.streaming.schemaInference", "true")
  source_df = (spark.readStream
                      .format("csv")
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .option("mergeSchema", "true")
                      .load(f"{base_dir}")
  )

  write_query = (source_df.writeStream
                          .format("delta")
                          .option("checkpointLocation", f"{base_dir}/chekpoint/invoices")
                          .option("mergeSchema", "true")
                          .outputMode("append")                          
                          .trigger(availableNow = True)
                          .toTable("dev.demo_db04.invoices_raw")
  )

ingest()  

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Check the records after ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.demo_db04.invoices_raw limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC #####6. Ingest some more data into the invoices directory which comes with an additional column

# COMMAND ----------

# MAGIC %fs cp /mnt/files/dataset_ch8/invoices_2021.csv /mnt/files/dataset_ch8/invoices/

# COMMAND ----------

# MAGIC %md
# MAGIC #####8. Ingest

# COMMAND ----------

ingest()

# COMMAND ----------

# MAGIC %md
# MAGIC #####9. Check the data 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM dev.demo_db04.invoices_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE dev.demo_db04.invoices_raw
