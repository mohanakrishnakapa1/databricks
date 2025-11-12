# Databricks notebook source
data = [(1, "abc@gmail.com"), (2, "bcd@gmail.com"), (3, "abc@gmail.com")]
schema = "ID int,email string"
df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.createOrReplaceTempView("email")

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,email from 
# MAGIC (select id,email,rank() over(partition by EMAIL order by Id) as rnk from email) a
# MAGIC where a.rnk>1

# COMMAND ----------

# MAGIC %md
# MAGIC ####using Native methods

# COMMAND ----------

from pyspark.sql.functions import col
display(df.groupBy("email").count().filter(col("count")>1).select(col("email")))

# COMMAND ----------


