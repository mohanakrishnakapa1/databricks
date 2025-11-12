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

# MAGIC %md
# MAGIC ###################collect_list

# COMMAND ----------

from pyspark.sql.functions import collect_list
df_Coll_lst=df_final.groupBy("Id","Name").agg(collect_list("Marks").alias("Marks_coll_list"))
df_Coll_lst.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #######collect_set.Main use is to remove the Duplicates.Check the last ID-4 ..100 will come only one time

# COMMAND ----------

from pyspark.sql.functions import collect_set
df_Coll_set=df_final.groupBy("Id","Name").agg(collect_set("Marks").alias("Marks_coll_list"))
df_Coll_set.show()

# COMMAND ----------

from pyspark.sql.functions import col
df_Coll_lst.select("Id","Name",col("Marks_coll_list")[1].alias("Maths")).show()

# COMMAND ----------


