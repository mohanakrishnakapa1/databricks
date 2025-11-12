# Databricks notebook source
# MAGIC %md
# MAGIC ## How to Optimize Joins
# MAGIC 1.Big Vs Small
# MAGIC
# MAGIC 2.Big Vs Big Table
# MAGIC
# MAGIC 3.Bucketing Strategy to load data faster
# MAGIC
# MAGIC Normal Join Data will be shuffled betwen Executors.
# MAGIC
# MAGIC Strategies to Optmize:
# MAGIC
# MAGIC 1. Shuffle HashJoin:
# MAGIC
# MAGIC        a.Shuffle
# MAGIC        b.Smaller Hashed
# MAGIC        c.Hashed dataset will be matched with Big Dataset
# MAGIC        d.Join ( Deparment Dataset for Ex)
# MAGIC     In this there no sorting happening and this one of reliaing Join if we have one Smaller Dataset.
# MAGIC
# MAGIC 2.Sort Merge:
# MAGIC
# MAGIC        a. shuffle
# MAGIC        b.Sort
# MAGIC        c.Merge
# MAGIC        Here Shuffle will happen , This is useful when we have Big datasets, WE have use this join carefully as we have shuffle operation involved.
# MAGIC
# MAGIC 3. Broadcast Join:
# MAGIC
# MAGIC        a.Broadsct smaller dataset
# MAGIC        b.Join (broadcast hashjoin)
# MAGIC        No shuffle and we can join with big table very efficiently.
# MAGIC       If the dataset morethan 10 MB it will throw the error, We can increase this to 8 GB.

# COMMAND ----------

# Disable AQE and Broadcast join

spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# Reading with Schema
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date"
df_emp = spark.read.format("csv").option("header",True).schema(_schema).load("/FileStore/tables/day7/emp_csv.txt")
 
# Read DEPT CSV data
_dept_schema = "department_id int, department_name string, description string, city string, state string, country string"
df_dept = spark.read.format("csv").option("header",True).schema(_dept_schema).load("/FileStore/tables/day7/department_data.txt")
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Big Vs Small Table

# COMMAND ----------

df_joined =df_emp.join(df_dept, on = df_emp.department_id == df_dept.department_id, how="left_outer")

# COMMAND ----------

df_joined.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Explain clearly see the SortMerge Join with shuffle

# COMMAND ----------



df_joined.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### if we use the broadcast join on Small Table we can Aviod Shuffle. So In this case broadcast Join will help Us.

# COMMAND ----------

# BroadCast Join

from pyspark.sql.functions import broadcast
df_broadcast =df_emp.join(broadcast(df_dept), on = df_emp.department_id == df_dept.department_id, how="left_outer")
df_broadcast.write.format("noop").mode("overwrite").save()

# COMMAND ----------

df_broadcast.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Buketing the Join Column will have better option.
# MAGIC
# MAGIC Suppose if we want to join based on city_id, Since we have buketing the city_id data, Then There is no Shuffle required in this case.
# MAGIC
# MAGIC suppose we city 1,2,3 part of buket1 4,5,6 are part of bucket2 in the city table
# MAGIC
# MAGIC We have sales table we create same buketing then we perform join on city_id then no need shuffle. I am going to do this using department_id.
# MAGIC
# MAGIC Buketing will work when we saveASTable.
# MAGIC

# COMMAND ----------

df_emp.write.format("csv").mode("overwrite").bucketBy(4,"department_id").option("header",True).saveAsTable("emp_bucket")

# COMMAND ----------

df_dept.write.format("csv").mode("overwrite").bucketBy(4,"department_id").option("header",True).saveAsTable("dept_bucket")

# COMMAND ----------

spark.sql("show tables in default").show()

# COMMAND ----------

#Read Tables
emp_bucket = spark.read.table("emp_bucket")
dept_bucket = spark.read.table("dept_bucket")

# COMMAND ----------

#join Bucket

df_bucketJoin = emp_bucket.join(dept_bucket, on = emp_bucket.department_id == dept_bucket.department_id, how="left_outer")
df_bucketJoin.write.format("noop").mode("overwrite").save()

# COMMAND ----------

df_bucketJoin.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Points to Note:
# MAGIC    #### 1. Joining column different than bucket column same Bucket size - shuffle on both table 
# MAGIC    #### 2. Joining column same one table in bucket - shuffle on non bucket table
# MAGIC    #### 3. Joining column same , Different bucket size - Shuffle on smaler bucket side
# MAGIC    #### 4. Joining column same, same bucket size - No Shuffle(Faster Join)
# MAGIC
# MAGIC #### 1. so it ver important to choose the correct bucket column and bucket size
# MAGIC #### 2. Decide effectively on number of buckets as too 

# COMMAND ----------


