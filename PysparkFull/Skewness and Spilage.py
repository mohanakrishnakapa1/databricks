# Databricks notebook source
# MAGIC %md
# MAGIC ### Dataskeness is basically unbalanced data and not evenly distributed with in the tasks
# MAGIC
# MAGIC #### Due to that one tasks will have have more data  few other tasks will sit ideal
# MAGIC
# MAGIC #### Spilage and Skewness comes hand to hand, 
# MAGIC
# MAGIC ####Types of Spilage
# MAGIC
# MAGIC   ###### 1.Spill Memory : Data which not fit in memory will be serialized and written to disk.
# MAGIC
# MAGIC   ###### 2.Spill Disk :  Serialized data will be stored in Disk. 
# MAGIC
# MAGIC   So Skewness will create problems as spark will need to read the Data from Disk and deserialize and perform the operations. So this will take time and hit performance.
# MAGIC
# MAGIC ### One way to fix the Skewness is repartitioning the Data. again if have huge data in one parition then it will lead another skewness.
# MAGIC
# MAGIC ### so we have to go with salting technique inorder to distribute the data evenly between partitions
# MAGIC
# MAGIC ### In Salting we will create a Salting key for Join, So We will add some random Numbers, So a random keys will be generated and data will be distributed Evently.
# MAGIC
# MAGIC ### This Doen't neeeded everywhere, Make sure to use only when you have issues of Memory due to spilage (mostly out of memory errors)
# MAGIC     
# MAGIC
# MAGIC ## WE have to see scenario by scenario and optimze.
# MAGIC
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

df_joined =df_emp.join(df_dept,on= df_emp.department_id==df_dept.department_id,how="left")

# COMMAND ----------

df_joined.write.format("noop").mode("overwrite").save()


# COMMAND ----------

from pyspark.sql.functions import spark_partition_id,count,lit

part_df=df_joined.withColumn("partition_num",spark_partition_id()).groupBy("partition_num").agg(count(lit(1)).alias("count_partitions"))
part_df.show()

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",16)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparing the Salt Technique

# COMMAND ----------

import random
from pyspark.sql.functions import udf

#udf to return a random number every time and add to employee as salt

@udf
def salt_udf():
    return random.randint(0,16)


#Salt Data Frame to add to deparment
salt_df = spark.range(0,16)  # why 16 as we have restricted shuffle paritions to 16
#salf_df.show()

# COMMAND ----------

#Salted Employee
from pyspark.sql.functions import lit, concat

salt_emp = df_emp.withColumn("salted_dept_id",concat("department_id",lit("_"),salt_udf()))

salt_emp.show()

# COMMAND ----------

#Salted dept

salt_dept = df_dept.join(salt_df,how="cross").withColumn("salted_dept_id",concat(df_dept["department_id"], lit("_"), salt_df["id"]))
salt_dept.show()

# COMMAND ----------

df_dept.schema

# COMMAND ----------

#Now Join the Both Salted DF
from pyspark.sql.functions import col

salted_Join_df = salt_emp.join(salt_dept,on=salt_emp.salted_dept_id==salt_dept.salted_dept_id,how="left_outer")

# COMMAND ----------


salted_Join_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id,count,lit

salt_part_df=salted_Join_df.withColumn("partition_num",spark_partition_id()).groupBy("partition_num").agg(count(lit(1)).alias("count_partitions"))
salt_part_df.show()

# COMMAND ----------


