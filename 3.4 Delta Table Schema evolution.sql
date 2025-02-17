-- Databricks notebook source
-- MAGIC %fs ls abfss://dbfs-container@adlssmiloeabronzedev001.dfs.core.windows.net

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC def setup():
-- MAGIC     spark.sql("CREATE CATALOG IF NOT EXISTS dev")
-- MAGIC     spark.sql("CREATE DATABASE IF NOT EXISTS dev.demo_db")
-- MAGIC     spark.sql("""CREATE OR REPLACE TABLE dev.demo_db.people_tbl(
-- MAGIC                         id INT,
-- MAGIC                         firstName STRING,
-- MAGIC                         lastName STRING
-- MAGIC                         ) USING DELTA""")
-- MAGIC     spark.sql("""INSERT INTO dev.demo_db.people_tbl
-- MAGIC                     SELECT id, fname, lname
-- MAGIC                     FROM json.`abfss://dbfs-container@adlssmiloeabronzedev001.dfs.core.windows.net/people.json`""")    
-- MAGIC
-- MAGIC spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")    
-- MAGIC setup()
-- MAGIC spark.sql("select * from dev.demo_db.people_tbl").display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. Manual schema evolution - New column at the end

-- COMMAND ----------

ALTER TABLE dev.demo_db.people_tbl ADD COLUMNS (birthDate STRING);

-- COMMAND ----------

INSERT INTO dev.demo_db.people_tbl
SELECT id, fname firstName, lname lastName, dob birthDate
FROM json.`abfss://dbfs-container@adlssmiloeabronzedev001.dfs.core.windows.net/people.json`

-- COMMAND ----------

select * from dev.demo_db.people_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Manual schema evolution - New column in the middle

-- COMMAND ----------

ALTER TABLE dev.demo_db.people_tbl ADD COLUMNS (phoneNumber STRING after lastName);

-- COMMAND ----------

INSERT INTO dev.demo_db.people_tbl
SELECT id, fname firstName, lname lastName, phone phoneNumber, dob birthDate
FROM json.`abfss://dbfs-container@adlssmiloeabronzedev001.dfs.core.windows.net/people_2.json`

-- COMMAND ----------

select * from dev.demo_db.people_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Cleanup and Setup for Automatic Schema Evolution

-- COMMAND ----------

-- MAGIC %python
-- MAGIC setup()
-- MAGIC spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
-- MAGIC spark.sql("select * from dev.demo_db.people_tbl").display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Automatic Schema Evolution - At Session level

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled = true

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Automatic schema evolution - New column at the end

-- COMMAND ----------

INSERT INTO dev.demo_db.people_tbl
SELECT id, fname firstName, lname lastName, dob birthDate
FROM json.`abfss://dbfs-container@adlssmiloeabronzedev001.dfs.core.windows.net/people_2.json` 

-- COMMAND ----------

select * from dev.demo_db.people_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Automatic schema evolution - New column in the middle
-- MAGIC For INSERT 
-- MAGIC 1. Either it doesn't work because of the column matching by position
-- MAGIC 2. Or it corrupts your data

-- COMMAND ----------

INSERT INTO dev.demo_db.people_tbl
SELECT id, fname firstName, lname lastName, phone phoneNumber, dob birthDate
FROM json.`abfss://dbfs-container@adlssmiloeabronzedev001.dfs.core.windows.net/people_2.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Automatic schema evolution - New column in the middle
-- MAGIC Works with MERGE INSERT

-- COMMAND ----------

MERGE INTO dev.demo_db.people_tbl tgt
USING (SELECT id, fname firstName, lname lastName, phone phoneNumber, dob birthDate FROM json.`abfss://dbfs-container@adlssmiloeabronzedev001.dfs.core.windows.net/people_3.json`) src
ON tgt.id = src.id
WHEN NOT MATCHED THEN
    INSERT *

-- COMMAND ----------

select * from dev.demo_db.people_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Cleanup and Setup for Automatic Schema Evolution at Table level

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC setup()
-- MAGIC spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
-- MAGIC spark.sql("select * from dev.demo_db.people_tbl").display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####6. Schema evolution - New column at the end

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import to_date
-- MAGIC
-- MAGIC people_2_schema = "id INT, fname STRING, lname STRING, dob STRING"
-- MAGIC
-- MAGIC people_2_df =  (spark.read.format("json").schema(people_2_schema)
-- MAGIC                     .load("abfss://dbfs-container@adlssmiloeabronzedev001.dfs.core.windows.net/people_2.json")
-- MAGIC                     .toDF("id", "firstName", "lastName", "birthDate"))
-- MAGIC
-- MAGIC (people_2_df.write
-- MAGIC       .format("delta")
-- MAGIC       .mode("append")
-- MAGIC       .option("mergeSchema", "true")
-- MAGIC       .saveAsTable("dev.demo_db.people_tbl")
-- MAGIC )

-- COMMAND ----------

select * from dev.demo_db.people_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Automatic schema evolution - New column in the middle

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import to_date
-- MAGIC
-- MAGIC people_3_schema = "id INT, fname STRING, lname STRING, phone STRING, dob STRING"
-- MAGIC
-- MAGIC people_3_df =  (spark.read.format("json").schema(people_3_schema)
-- MAGIC                     .load("abfss://dbfs-container@adlssmiloeabronzedev001.dfs.core.windows.net/people_3.json")
-- MAGIC                     .toDF("id", "firstName", "lastName", "phoneNumber", "birthDate"))
-- MAGIC
-- MAGIC (people_3_df.write
-- MAGIC       .format("delta")
-- MAGIC       .mode("append")
-- MAGIC       .option("mergeSchema", "true")
-- MAGIC       .saveAsTable("dev.demo_db.people_tbl")
-- MAGIC )

-- COMMAND ----------

select * from dev.demo_db.people_tbl
