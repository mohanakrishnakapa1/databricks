# Databricks notebook source
# Check default Parallism

spark.sparkContext.defaultParallelism

# COMMAND ----------

#check Default Partiotions

df.rdd.getNumPartitions()

# COMMAND ----------

#Check Explain/Query/Execcution plan and we can optimize the operations where ever required

df_sum.explain()

# COMMAND ----------

#Default shuffle parttions in spark

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# Disable AQE and Broadcast join

spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

# Write data for performance Benchmarking, This will not store the data anywhere but it runs with all the data, So inorder to checking the benchmark without storing we can use this command.

emp_avg.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### To see the TAbles from database, right now I am using default database

# COMMAND ----------

spark.sql("show tables in default").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark Catalog (Metadata)  - in-memory/Hive. In Databricks default set to Hive/ Standalone Spark Environment it will be set to in-memory

# COMMAND ----------


spark.conf.get("spark.sql.catalogImplementation")

# COMMAND ----------

#Show Database
db = spark.sql("show databases")
db.show()

#Show Tables
spark.sql("show tables in default").show()

# Show all the metadata detaisl

spark.sql("describe extended emp_final").show()

# COMMAND ----------

## To check Medata File Name

%sql
select min(department_id),max(department_id),_metadata.file_name from emp_records
group by _metadata.file_name
order by min(department_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Table using Files Directly

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
