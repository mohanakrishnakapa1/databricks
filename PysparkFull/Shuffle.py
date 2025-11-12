# Databricks notebook source
# MAGIC %md
# MAGIC https://www.youtube.com/watch?v=PHVFDgk3lok&list=PL2IsFZBGM_IHCl9zhRVC1EXTomkEp_1zm&index=19
# MAGIC
# MAGIC As we seen in the DAG(Check DAG Notepad) shuffle will create more stages
# MAGIC
# MAGIC what generally spark does is it will execute all the Narrow Tranformations as possible and write the shuffle data are Serialized in Tungsten Binary format (unsafe row).
# MAGIC
# MAGIC These files can directly be read in memory thus improving the read performance.
# MAGIC
# MAGIC The next stage Spark will read these shuffle files and Narrow Tranformation data from pipeline and it will complete the stage.
# MAGIC
# MAGIC ****important point is these files will written to Disk**** and sent over to next pipeline/stage to complete the operations, So reading of these files will decrease the performance. So always shfulle is a costly operation.
# MAGIC
# MAGIC **** But sometimes we can aviod the shuffle operations **** So we have deal with number of shuffle tasks andto  Minimize the shuffle using Techniques by Checking the DAG.
# MAGIC
# MAGIC Change the Defalut shuffle paritions using below settings if we have less partitions or if we don't need too much shuffle.***
# MAGIC
# MAGIC spark.conf.set("spark.sql.shuffle.partitions",100)  -- Here we are reducing the size from 200 to 100
# MAGIC
# MAGIC spark.conf.set("spark.sql.shuffle.partitions",16)  -- Here we are reducing the size from 200 to 16
# MAGIC
# MAGIC We have to be very careful while setting this property, Can lead the errors and Performacne decrease if we set wrong number.
# MAGIC
# MAGIC
# MAGIC Check the   ***Shuffle write Size/Records** from DAG --> Tasks to check the Shuffle data or size or records.
# MAGIC
# MAGIC This setting will be much lower if we are dealing with partition Data. as every task is reading 1 partitions data and no need of shuffle while aggregation.
# MAGIC
# MAGIC
# MAGIC Few Important Points:
# MAGIC
# MAGIC 1. Good Shuffle -- Aviod Uncessary Shuffle
# MAGIC 2. Repartition Data -- Can reduce the Shuffle
# MAGIC 3. Filter -- Filter the data before shuffle -- This will reduce the Shuffle Data size.
# MAGIC
# MAGIC

# COMMAND ----------

# Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Optimizing Shuffles")
    .master("local[*]")
    .config("spark.cores.max", 16)
    .config("spark.executor.cores", 4)
    .config("spark.executor.memory", "512M")
    .getOrCreate()
)

spark

# COMMAND ----------

# Check Spark defaultParallelism

spark.sparkContext.defaultParallelism

# COMMAND ----------

# Disable AQE and Broadcast join

spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

_schema = "first_name string, last_name string, job_title string, dob string, email string, phone string, salary double, department_id int"

emp = spark.read.format("csv").schema(_schema).option("header", True).load("/FileStore/tables/pysparkfull/day19/employee_records.txt")

# COMMAND ----------

from pyspark.sql.functions import avg
emp_avg = emp.groupBy("department_id").agg(avg("salary").alias("avgsal"))

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------


