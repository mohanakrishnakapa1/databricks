# Databricks notebook source
df_sales = spark.read.parquet("/FileStore/tables/deltalake/sales_data.parquet")

# COMMAND ----------

display(df_sales)

# COMMAND ----------

#Write data as hive table

df_sales.write.format("parquet").mode("overwrite").option("path","/FileStore/tables/deltalake/hivetable/sales_data.parquet").saveAsTable("sales_parquet")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/deltalake/hivetable/

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in default

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended sales_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try update Parquet table which we can't update

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from default.sales_parquet where trx_id='1734117021'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update  default.sales_parquet set amount=0 where trx_id='1734117021'

# COMMAND ----------

# MAGIC %md
# MAGIC ## write same data inform of Delta table and try update the table

# COMMAND ----------

df_sales.write.format("delta").mode("overwrite").option("path","/FileStore/tables/deltalake/delta/sales").saveAsTable("sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in default

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended default.sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### delta lake save metadata in the form Json. To supoort DML,DDL operations and Audit logs and Time travel.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/FileStore/tables/deltalake/delta/sales/_delta_log"

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/deltalake/delta/sales/_delta_log/00000000000000000000.json")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Check how the Versioning the saved

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history sales

# COMMAND ----------

# MAGIC %sql
# MAGIC update  default.sales set amount=0 where trx_id='1734117021'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.sales where trx_id='1734117021'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/FileStore/tables/deltalake/delta/sales/_delta_log"

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/deltalake/delta/sales/_delta_log/00000000000000000001.json")


# COMMAND ----------

# MAGIC %sql
# MAGIC describe history default.sales
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Pyspark

# COMMAND ----------

df_sales_delta = spark.read.table("sales")
#another way
#df_sales_delta = spark.read.format("delta").load("location")

display(df_sales_delta.where ("trx_id=1734117021"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the particular version of the dela table

# COMMAND ----------

#here we are reading the data version 0, Before update. If don't mention the version it will latest version data.Check Amount column
df_sales_delta = spark.read.table("sales@v0")


display(df_sales_delta.where ("trx_id=1734117021"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales@v0 where trx_id="1734117021"

# COMMAND ----------

# MAGIC %md
# MAGIC ### add a new Column and Schema Evaolution **

# COMMAND ----------

df_new = spark.sql("select s.*,current_timestamp() as time_now from sales@v1 as s where trx_id='1734117021'")
display(df_new)

# COMMAND ----------

# Apeend data to existing delta table

df_new.write.format("delta").mode("append").option("mergeSchema",True).option("path","/FileStore/tables/deltalake/delta/sales").saveAsTable("sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history default.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.sales 

# COMMAND ----------

# Reading Deltatable using Delta libraries
from delta import DeltaTable

df = DeltaTable.forName(spark,"sales")
display(df.history())

# COMMAND ----------

#Converting a parquest to delta 

DeltaTable.isDeltaTable(spark,"/FileStore/tables/deltalake/hivetable/sales_data.parquet")

# COMMAND ----------

DeltaTable.isDeltaTable(spark,"/FileStore/tables/deltalake/delta/sales")

# COMMAND ----------

#Convert

DeltaTable.convertToDelta(spark,"parquet.`/FileStore/tables/deltalake/hivetable/sales_data.parquet`")

# COMMAND ----------

DeltaTable.isDeltaTable(spark,"/FileStore/tables/deltalake/hivetable/sales_data.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ### You still table as parquet we have create log file but the we have to convert

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc extended default.sales_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta sales_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc extended default.sales_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revert back to Previous Version
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC RESTORE TABLE SALES TO VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc history default.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from sales where trx_id='1734117021'

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACCUM to Clean the VErsioning and it will improve the Space management and Performance, Very dangourous command.becareful

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled","false")
dt = DeltaTable.forName(spark,"sales")
dt.vacuum(0)

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/deltalake/delta/sales"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Eventhough We see all the versions but we can't query the data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history sales

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select * from sales where trx_id='1734117021'
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select * from sales@v0 where trx_id='1734117021'

# COMMAND ----------

#converting delta to parquet
# Load the Delta table
delta_df = spark.read.format("delta").load("/path/to/delta_table")

# Write to Parquet
delta_df.write.format("parquet").save("/path/to/output/parquet_table")

