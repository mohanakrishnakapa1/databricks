# Databricks notebook source
drivers_day1_df=spark.read.option("inferSchema",True).json("/FileStore/tables/2021-03-28/drivers.json")\
.filter("driverId <=10") \
.select("driverId","dob","name.forename","name.surname")
drivers_day1_df.show(3)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df=spark.read.option("inferSchema",True).json("/FileStore/tables/2021-03-28/drivers.json")\
.filter("driverId between 6 and 15") \
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))
drivers_day2_df.show(3)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table f1_demo.driver_merge(
# MAGIC driverId Int,
# MAGIC dob Date,
# MAGIC forename STRING,
# MAGIC surname string,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %md
# MAGIC ####Day1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into f1_demo.driver_merge tgt
# MAGIC using drivers_day1 upd
# MAGIC on tgt.driverId=upd.driverId
# MAGIC when MATCHED 
# MAGIC THEN
# MAGIC update set tgt.dob=upd.dob,
# MAGIC           tgt.forename=upd.forename,
# MAGIC           tgt.surname=upd.surname,
# MAGIC           tgt.updateddate=current_timestamp
# MAGIC
# MAGIC when NOT MATCHED 
# MAGIC THEN 
# MAGIC insert (driverId,dob,forename,surname,createddate) VALUES (driverId,dob,forename,surname,current_timestamp)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.driver_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ######Day2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into f1_demo.driver_merge tgt
# MAGIC using drivers_day2 upd
# MAGIC on tgt.driverId=upd.driverId
# MAGIC when MATCHED 
# MAGIC THEN
# MAGIC update set tgt.dob=upd.dob,
# MAGIC           tgt.forename=upd.forename,
# MAGIC           tgt.surname=upd.surname,
# MAGIC           tgt.updateddate=current_timestamp
# MAGIC
# MAGIC when NOT MATCHED 
# MAGIC THEN 
# MAGIC insert (driverId,dob,forename,surname,createddate) VALUES (driverId,dob,forename,surname,current_timestamp)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.driver_merge

# COMMAND ----------


