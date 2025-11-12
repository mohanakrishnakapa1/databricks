# Databricks notebook source
# MAGIC %md
# MAGIC ### Increamental load Files Excatly once
# MAGIC
# MAGIC Scale upto Millions of Files.
# MAGIC
# MAGIC Schema evaloution
# MAGIC
# MAGIC Support Streaming and Batch Mode.

# COMMAND ----------

# MAGIC %md
# MAGIC Uses Check point location to track the files.
# MAGIC
# MAGIC Below is sample Volume used as Checkpoint location.
# MAGIC dbutils.fs.mkdir("/Volumes/dev/bronze/landing/checkping/autoloader")
# MAGIC
# MAGIC Maintain this information rockDB
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### File Detection Modes in Autoloader -- Uses RocksDB to store checkpoint information
# MAGIC
# MAGIC     Directory Listing --> (uses API calls to detect new files)
# MAGIC     File Notification (uses Queue services -require cloud permission)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read stream

# COMMAND ----------

df = (

  spark.readStream.
  format("cloudFiles")
  .option("cloudFiles.format","csv")
  .option("pathGlobFilter,"*.csv")
  .option("header","true")
  .option("cloudFiles.schemaHints","Quantity int,unitprice double" )  -- tHis schema hints can be defined for particular columns. If we want we can use Schema option as well for all the columns
  .option("cloudFiles.schemaLocation","/Volumes/dev/bronze/landing/checkping/autoloader/1")  -- This option is very important as the Schema details will stored in this location
  .load("/Volumes/dev/bronze/landing/checkping/autoloader_input/*/")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write stream

# COMMAND ----------

from pyspark.sql.functions import col

(

    df.withColumn("__file",col("metadata.file_name")) -- Created new columns while loading
    .writeStream
    .option("checkPointLocation,"/Volumes/dev/bronze/landing/autoloader/1/"
            ) 
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("dev.bronze.invoice_al_1")
)

# COMMAND ----------


Schema Evaolution:
  
Mode	Behavior on reading new column
addNewColumns (default)  -->	Stream fails. New columns are added to the schema. Existing columns do not evolve data types.ITs fails first time but update the schema in checkpoint location
option("mergeSchema",True)

rescue	Schema is never evolved and stream does not fail due to schema changes. All new columns are recorded in the rescued data column.
df.readStream.option("cloudFiles.schemaEvaloutionMode","rescue")

failOnNewColumns	Stream fails. Stream does not restart unless the provided schema is updated, or the offending data file is removed. 


none	Does not evolve the schema, new columns are ignored, and data is not rescued unless the rescuedDataColumn option is set. Stream does not fail due to schema changes.new columns will be ignored
df.readStream.option("cloudFiles.schemaEvaloutionMode","none")


# COMMAND ----------


