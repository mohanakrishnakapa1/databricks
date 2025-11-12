# Databricks notebook source
# MAGIC %md
# MAGIC ## AQE will check the Most optimization before Execution. 
# MAGIC
# MAGIC ### Performance Tuning
# MAGIC     #### Caching Data In Memory
# MAGIC     #### Other Configuration Options
# MAGIC     #### Join Strategy Hints for SQL Queries
# MAGIC     #### Coalesce Hints for SQL Queries
# MAGIC     #### Adaptive Query Execution
# MAGIC       #### Coalescing Post Shuffle Partitions
# MAGIC       #### Spliting skewed shuffle partitions
# MAGIC       #### Converting sort-merge join to broadcast join
# MAGIC       #### Converting sort-merge join to shuffled hash join
# MAGIC       #### Optimizing Skew Join
# MAGIC
# MAGIC ### Following example explains the Join example, If we disable and execute the Join then we can see some spilage as well as Default parition 200
# MAGIC
# MAGIC ### Next we enabled AQE and execute and the spark will automatically decide the default paritions and Join strategy. 
# MAGIC
# MAGIC ### This is only example Even Coalesce and repartion can decide by spark if we enable AQE and different join strategies can decide by spark.
# MAGIC
# MAGIC ### AQE is basically spark can give performance and we don't need perform any salting,Joins like broadcast..etc
# MAGIC
# MAGIC ### But in few cases we need to decide the partitions..etc case to case it will vary
# MAGIC

# COMMAND ----------

# Disable AQE and Broadcast join

spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# Reading with Schema
_schema = "first_name string, last_name string, job_title string, dob string, email string,phone string, salary double, department_id int"
df_emp = spark.read.format("csv").option("header",True).schema(_schema).load("/FileStore/tables/day7/employee_records.txt")
 
# Read DEPT CSV data
_dept_schema = "department_id int, department_name string, description string, city string, state string, country string"
df_dept = spark.read.format("csv").option("header",True).schema(_dept_schema).load("/FileStore/tables/day7/department_data.txt")

# COMMAND ----------

# Join Datasets

df_joined = df_emp.join(df_dept, on=df_emp.department_id==df_dept.department_id, how="left_outer")

# COMMAND ----------

df_joined.write.format("noop").mode("overwrite").save()

# COMMAND ----------


#Explain Plan

df_joined.explain()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable AQE and  Test

# COMMAND ----------

# Coalescing post-shuffle partitions - remove un-necessary shuffle partitions
# Skewed join optimization (balance partitions size) - join smaller partitions and split bigger partition

spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)

# Fix partition sizes to avoid Skew

spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "8MB") #Default value: 64MB
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "10MB") #Default value: 256MB

# COMMAND ----------

# Converting sort-merge join to broadcast join

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

# COMMAND ----------

# Join Datasets

df_joined = df_emp.join(df_dept, on=df_emp.department_id==df_dept.department_id, how="left_outer")

# COMMAND ----------

df_joined.write.format("noop").mode("overwrite").save()

# COMMAND ----------

df_joined.explain()

# COMMAND ----------


