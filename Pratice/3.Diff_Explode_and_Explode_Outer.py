# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
from pyspark.sql.functions import explode,explode_outer
data=[(1,"Sagar",[20,30,10]),(2,"Mohan",[40,30]),(3,"Sumanth",[]),(4,"Radha",[50,100,1000])]
schema=StructType([StructField("Id",IntegerType(),True),StructField("Name",StringType(),True),StructField("Marks",ArrayType(IntegerType()),True)])
df=spark.createDataFrame(data,schema=schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###################Try To explode the arrary using explode Observer the Null will ignored
# MAGIC

# COMMAND ----------

df.select("Id","Name",explode("Marks")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###############use explode_outer func to get Null values as well

# COMMAND ----------

df.select("Id","Name",explode_outer("Marks")).show()

# COMMAND ----------


