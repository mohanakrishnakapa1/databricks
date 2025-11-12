# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Create necessary schemas/Database as we are using Hive_metastore from community addition

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog();

# COMMAND ----------

#dbutils.fs.rm("dbfs:/FileStore/tables/warehouse/source_crm/deltacust",recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer Scripts

# COMMAND ----------

# MAGIC %sql
# MAGIC drop schema if exists bronze;
# MAGIC drop schema if exists silver;
# MAGIC drop schema if exists gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema IF NOT EXISTS bronze;
# MAGIC
# MAGIC create schema IF NOT EXISTS silver;
# MAGIC
# MAGIC create schema IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/warehouse/source_crm/

# COMMAND ----------

df_cust=spark.read.csv("dbfs:/FileStore/tables/warehouse/source_crm/cust_info.csv",header=True,inferSchema=True)
df_cust.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/warehouse/source_crm/deltacust")

df_prd=spark.read.csv("dbfs:/FileStore/tables/warehouse/source_crm/prd_info.csv",header=True,inferSchema=True)
df_prd.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/warehouse/source_crm/deltaprd")


df_sales=spark.read.csv("dbfs:/FileStore/tables/warehouse/source_crm/sales_details.csv",header=True,inferSchema=True)
df_sales.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/warehouse/source_crm/deltasale")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.crm_cust_info
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/FileStore/tables/warehouse/source_crm/deltacust";
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.crm_prd_info
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/FileStore/tables/warehouse/source_crm/deltaprd";
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.crm_sales_details
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/FileStore/tables/warehouse/source_crm/deltasale";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.crm_cust_info limit 2;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.crm_prd_info limit 2;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.crm_sales_details limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases in hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in bronze
