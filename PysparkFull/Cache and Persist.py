# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Cache and Persist
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Default Storage level for Cache is -- MEMORY_AND_DISK
# MAGIC
# MAGIC Persist We have different level of Settings:
# MAGIC
# MAGIC If we want Different setting then we need to use Persist and Persist will serialize the data. So Data accessing will be more fast in Cache.
# MAGIC
# MAGIC MEMORY_ONLY, MEMORY_AND_DISK, MEMORY_ONLY_SER, MEMORY_AND_DISK_SER, DISK_ONLY, MEMORY_ONLY_2, MEMORY_AND_DISK_2
# MAGIC
# MAGIC
# MAGIC
# MAGIC ***IF run any command on Cache data the operation will be very fast.Check the spark UI for Timining, also it will not hit the Scan_csv as it is going to return the data from Cache.
# MAGIC
# MAGIC
# MAGIC To Cache the data: Even partial cache is allowed based on conditions.check the Notebook
# MAGIC
# MAGIC Next Example if Take the cache into another DF like below, But when call the data for original dataframe but still it will get the data from Cache
# MAGIC emp_cache = emp.cache()
# MAGIC emp_cache.count()

# COMMAND ----------

# Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Understand Caching")
    .master("local[*]")
    .config("spark.executor.memory", "512M")
    .getOrCreate()
)

spark

# COMMAND ----------

_schema = "first_name string, last_name string, job_title string, dob string, email string, phone string, salary double, department_id int"

emp = spark.read.format("csv").schema(_schema).option("header", True).load("/FileStore/tables/pysparkfull/day19/employee_records.txt")

# COMMAND ----------


# Once we run the command it will hit csv and get some sample data. Check the SQL/Dataframe in Spark UI
emp.where("salary > 6000").show()

# COMMAND ----------


#Cache the Dataframe and Cache directly would not put the data into Memory. So we have to Run Count or a Write Mechanism inorder to put Full data in to Memory.
#If we trigger only data based on Conidtion emp.where("salary > 6000") it gonna add only those records and it not going to push all the dataframe data***
#see the next cell to understand how it represent in SparkUI
# emp.cache().count()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ID	RDD Name	Storage Level	Cached Partitions	Fraction Cached	Size in Memory	Size on Disk
# MAGIC 11	FileScan csv [first_name#6,last_name#7,job_title#8,dob#9,email#10,phone#11,salary#12,department_id#13] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[dbfs:/FileStore/tables/pysparkfull/day19/employee_records.txt], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<first_name:string,last_name:string,job_title:string,dob:string,email:string,phone:string,s...	Disk Memory Deserialized 1x Replicated	8	100.00%	74.9 MiB	0.0 B

# COMMAND ----------

# MAGIC %md
# MAGIC # Storage Level
# MAGIC
# MAGIC #Disk Memory Deserialized 1x Replicated

# COMMAND ----------

# IF run any command on Cache data the operation will be very fast.Check the spark UI for Timining, also it will not hit the Scan_csv as it is going to return the data from Cache.
emp.where("salary>60000").show()

# COMMAND ----------

#to remove the cache run the below command

emp.unpersist()

# COMMAND ----------

#Next Example if Take the cache into another DF like below, But when call the data for original dataframe but still it will get the data from Cache
emp_cache = emp.cache()
emp_cache.count()



# COMMAND ----------


#The below Command still get the data from cache
emp.where("salary>110000").show()

# COMMAND ----------

# If we not done the cache properly then the Query will hit if are trying to execute the orginal dataframe
emp_cache_part = emp.where("salary>900000").cache()
emp_cache_part.count()


# COMMAND ----------

#run the command and check SparkUI
emp.where("salary<800000").show()

# COMMAND ----------

emp_cache.unpersist()
emp_cache_part.unpersist()
emp.unpersist()

# COMMAND ----------

# If we want Different setting then we need to use Persist and Persist will serialize the data. So Data accessing will be more fast in Cache.
# MEMORY_ONLY, MEMORY_AND_DISK, MEMORY_ONLY_SER, MEMORY_AND_DISK_SER, DISK_ONLY, MEMORY_ONLY_2, MEMORY_AND_DISK_2
import pyspark

df_persist = emp.persist(pyspark.StorageLevel.MEMORY_ONLY_2)
#or
#df_persist = emp.persist(pyspark.StorageLevel.MEMORY_ONLY_DISK)

# COMMAND ----------

#nooop will not do anything but it will run all the data
df_persist.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# MAGIC #Storage Level for MEMORY_ONLY_2
# MAGIC ##Memory Serialized 2x Replicated

# COMMAND ----------

#Remove all the Cache
spark.catalog.clearCache()

# COMMAND ----------


