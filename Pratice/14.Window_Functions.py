# Databricks notebook source
# MAGIC %md
# MAGIC ###Window.partitionBy("location").orderBy(col("alt").desc())

# COMMAND ----------

df=spark.read.csv("/FileStore/tables/2021-03-21/circuits.csv",inferSchema=True,header=True)

# COMMAND ----------

df.show(2)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
w=Window.partitionBy("country").orderBy("country")

# COMMAND ----------

df.withColumn("row_num",row_number().over(w)).show()

# COMMAND ----------

df = df.withColumn("row_number", row_number().over(w))

# Rank within each partition
df = df.withColumn("rank", rank().over(Window.partitionBy('country','location').orderBy('location')))

# Dense rank within each partition
df = df.withColumn("dense_rank", dense_rank().over(Window.partitionBy('country').orderBy('location')))

# Percent rank within each partition
df = df.withColumn("percent_rank", percent_rank().over(w))

# COMMAND ----------

df.show()

# COMMAND ----------

df.createOrReplaceTempView("cir_tocheck_rank_denserank")

# COMMAND ----------

spark.sql("""select circuitId,location,country,rank() over(partition by country order by location) from cir_tocheck_rank_denserank""").show()

# COMMAND ----------

df.withColumn("ntile", ntile(4).over(w)).drop("rank","percent_rank").show()

# COMMAND ----------

df.withColumn("ntile", lead("alt").over(w)).drop("rank","percent_rank").show()

# COMMAND ----------

df.withColumn("ntile", lag("alt").over(w)).drop("rank","percent_rank").show()

# COMMAND ----------

df.withColumn("last_value", last("alt").over(w)).drop("rank","percent_rank").show()

# COMMAND ----------

df.withColumn("last_value", first("alt").over(w)).drop("rank","percent_rank").show()

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,df = 

