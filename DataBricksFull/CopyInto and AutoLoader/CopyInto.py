# Databricks notebook source
# MAGIC %md
# MAGIC ### This is very useful when we want to load different data files in Delta Lake Tables.
# MAGIC
# MAGIC #### Best part is retirable and Idempotent.
# MAGIC
# MAGIC ### Idempotent --> Means when we run for same processed files it won't copy the same files once agian. Once we load file and if we load same once again then it won't process. This is also called only once.
# MAGIC
# MAGIC ## this is very useful when we have 1000's of files, but when we have Millinos of files with nexted folder structure then databricks recommentded to use autoloader.

# COMMAND ----------

# MAGIC %sql
# MAGIC Create volume dev.bronze.landing
# MAGIC COMMENT "this is managed landing";

# COMMAND ----------

# MAGIC %md
# MAGIC ### Just Check the below Commands as we are unable to crate volumes and unity catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ### just create a place holder table with out data. as we need this table to load the data.

# COMMAND ----------

# MAGIC %sql
# MAGIC create table dev.bronze.invoice_cp;

# COMMAND ----------

# MAGIC %md
# MAGIC ### The below specify the Schema options for source. We must specify the mergeSchema since we have not defined source schema.
# MAGIC
# MAGIC FORMAT_OPTIONS(
# MAGIC     mergeSchema = 'true',
# MAGIC     'header'='true'
# MAGIC )
# MAGIC
# MAGIC ### the below is Target schema options
# MAGIC
# MAGIC copy_options (
# MAGIC     'mergeSchema' = 'true'
# MAGIC )

# COMMAND ----------


-- use cpoy Into to load the data into place holder table
copy into dev.bronze.invoice_cp
from "/volumes/dev/bronze/landing/input"
FILEFORMAT = csv
PATTERN ='*csv'
FORMAT_OPTIONS(
    mergeSchema = 'true',
    'header'='true'
)
copy_options (
    'mergeSchema' = 'true'
)
;

# COMMAND ----------

# MAGIC %md
# MAGIC ### There is folder created with name ** _copy_into_log ** and maintains metadata. this ensure no repated copy and make copy inot idempotent.

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to copy only certain columns using copy Into

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create target Table first with schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create table dev.bronze.invoice_cp_alt(
# MAGIC
# MAGIC   invoiceNo string,
# MAGIC   stockCode string,
# MAGIC   Quantity double,
# MAGIC   _insert_date timestamp
# MAGIC )

# COMMAND ----------

## We don't need to mention Target schema.
copy into dev.bronze.invoice_alt
from (
    select inoviceNo,stockCode,cast(Quantity as double) Quantity,current_timestamp _insert_date
    "/volumes/dev/bronze/landing/input"
    
    )
FILEFORMAT = csv
PATTERN ='*csv'
FORMAT_OPTIONS(
    mergeSchema = 'true',
    'header'='true'
)
;
