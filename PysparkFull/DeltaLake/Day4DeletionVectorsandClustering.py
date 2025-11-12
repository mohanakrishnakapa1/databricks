# Databricks notebook source
# MAGIC %md
# MAGIC ## When Normal Scenarios if delete the one single from DELTA Table** then Entire Parquet file will be rewritten. This is an optimization issue.
# MAGIC ## To aviod this we are using Deletion Vectors, This will update the Flag for Rows dleted and don't rewritten entire File.
# MAGIC ## Whenever we run the OPTIMIZE command then all the rows marked as FLAG will be deleted then file will be rewritten
# MAGIC

# COMMAND ----------

# Write the data in form of delta table
df = spark.read.csv(path="/data/input/sales/sales.csv", inferSchema=True, header=True)
df.repartition(16).write.format("delta").mode("overwrite").partitionBy("country").option("path", "/data/output/sales_delta_partitioned/").saveAsTable("sales_delta_partitioned")
     

# COMMAND ----------

# MAGIC %sql
# MAGIC create Table default.sales
# MAGIC select * from
# MAGIC read_files
# MAGIC (
# MAGIC 'dbfs:/databricks-datasets/online_retail/data-001/data.csv',
# MAGIC header => true,
# MAGIC format => 'csv'
# MAGIC
# MAGIC )

# COMMAND ----------

#Read as Delta Table
df_sales = spark.read.format("csv").option("header",True).option("inferSchema",True).load("dbfs:/databricks-datasets/online_retail/data-001/data.csv")
df_sales.write.format("delta").option("path","/FileStore/tables/deletionVector").mode("overwrite").saveAsTable("salesdt")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesdt

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended default.salesdt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Above Table Properties are Empty But in Licensed version we can see property like delta.enableDeletionVectors=true
# MAGIC ### Check numDeletionVectorsRemoved: "0" property from Operation Metrics in decribe history default.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE default.salesdt SET TBLPROPETIES (delta.enableDeletionVectors = false);

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from default.sales where invoiceNo='540644'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history default.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE default.salesdt SET TBLPROPETIES (delta.enableDeletionVectors = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from default.sales where invoiceNo='536368'

# COMMAND ----------

# MAGIC %md
# MAGIC ### There demerit while we are partitioning and Zordering. Entire table data has to rewrite.
# MAGIC ## But Once we enabling clustering then we don't need to rewrite the data the incremental data automatically adjusted as per column we mentioned
# MAGIC ### Scenarios where we can use Clustering
# MAGIC #### a.Table Often filterd by high cardinality columns
# MAGIC #### b.Table with significant skew in data distribution
# MAGIC #### c.Table that drow quickly and require tuning efforts
# MAGIC #### d.Table access patterns that change over time
# MAGIC #### e.where to many partitions in table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE default.sales CLUSTER BY (INVOICENO);

# COMMAND ----------

desc history defalut.sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### You can see CLUSTERBY Defined

# COMMAND ----------

#Disable Cluster
%sql
ALTER TABLE default.sales CLUSTER BY (NONE);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create cluster on New Table

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC create Table default.sales_ct CLUSTER BY (INVOICENO)
# MAGIC select * from
# MAGIC read_files
# MAGIC (
# MAGIC 'dbfs:/databricks-datasets/online_retail/data-001/data.csv',
# MAGIC header => true,
# MAGIC format => 'csv'
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### We can have More than One Column in Cluster only limitation is column should be present in first 32 Columns

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC create Table default.sales_ct CLUSTER BY (INVOICENO,COUNTRY)
# MAGIC select * from
# MAGIC read_files
# MAGIC (
# MAGIC 'dbfs:/databricks-datasets/online_retail/data-001/data.csv',
# MAGIC header => true,
# MAGIC format => 'csv'
# MAGIC
# MAGIC )

# COMMAND ----------


