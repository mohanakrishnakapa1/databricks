# Databricks notebook source
# MAGIC %md
# MAGIC #######Create Database

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/delta/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/delta/demo"

# COMMAND ----------

result_df=spark.read.option("inferSchema",True).json("/FileStore/tables/2021-03-28/results.json")

# COMMAND ----------

result_df.show(2)

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

#load to file location
result_df.write.mode("overwrite").save("/mnt/delta/demo/results_external")

# COMMAND ----------

#create table on external file

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external
# MAGIC using delta
# MAGIC location "/mnt/delta/demo/results_external"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_demo.results_external

# COMMAND ----------

#Read method
spark.read.format("delta").load("/mnt/delta/demo/results_external").show(3)

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").partitionBy("constructorID").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ###usign python

# COMMAND ----------



# COMMAND ----------


