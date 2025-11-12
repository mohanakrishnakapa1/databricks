# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
from pyspark.sql.functions import explode,explode_outer
data=[(1,"Sagar",[20,30,10]),(2,"Mohan",[40,30]),(3,"Sumanth",[]),(4,"Radha",[50,100,100])]
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

df_final=df.select("Id","Name",explode_outer("Marks").alias("Marks"))
df_final.show()

# COMMAND ----------

df_final.groupBy("Id","Name").sum("Marks").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ################If there more 20 columns

# COMMAND ----------

df_final.groupBy(df_final.columns).sum("Marks").show()

# COMMAND ----------

df_final.groupBy(df_final.columns[0:2]).sum("Marks").show()

# COMMAND ----------

df_final.createOrReplaceTempView("stu")

# COMMAND ----------

# MAGIC %md
# MAGIC ################using groupbyall in SparkSQL,Metion group by all at the end
# MAGIC

# COMMAND ----------

spark.sql("select id,name,sum(marks) from stu group by all").show()

# COMMAND ----------


