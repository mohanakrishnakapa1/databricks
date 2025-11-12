# Databricks notebook source
# MAGIC %md
# MAGIC https://www.youtube.com/watch?v=RlLWMlDeS04&list=PL2IsFZBGM_IHCl9zhRVC1EXTomkEp_1zm&index=18
# MAGIC When there is Shuffle or Exchange state It will create a new stage.
# MAGIC
# MAGIC df_1 --> 0 Stage --> 8 Tasks reading as we have partitions       
# MAGIC
# MAGIC df_2 --> 1 Stage --> 8 Tasks reading as we have partitions
# MAGIC
# MAGIC Next stage 2 shuffle will happen Due to repartion a data shuffle will happpen.
# MAGIC
# MAGIC df_3 --> 2 stage --> Due Repartion and We need 5 Tasks as we have 5 Partitions
# MAGIC
# MAGIC df_4 --> 2 stage --> Due Repartion and We need 7 Tasks as we have 7 Partitions
# MAGIC
# MAGIC df_join --> this is also need shuffle data due to join and it will take default shuffle partition that is 200
# MAGIC
# MAGIC df_sum --> 1 stage and 1 Task - aggregation
# MAGIC
# MAGIC **Total 229 tasks which is what we seen in the DAG **
# MAGIC
# MAGIC df_sum.explain() -- This is to get Explain plan --> Read the Explain plan from the bottom and it can be used to Optimize lot of things.
# MAGIC
# MAGIC ****Skip Stage***
# MAGIC In few Cases we can Skip stages in Explain Plan, If we do the union with df_sum, Since df_sum computation is already done so it will take advantage of the and skip df_sum Computation and only do the Union Computation.
# MAGIC
# MAGIC df_union = df_sum.union(df_4)
# MAGIC
# MAGIC df_union.show()
# MAGIC

# COMMAND ----------

print("Hi")

# COMMAND ----------

# Disable AQE and Broadcast join

spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# Check default Parallism

spark.sparkContext.defaultParallelism

# COMMAND ----------

# Create dataframes

df_1 = spark.range(4, 200, 2)
df_2 = spark.range(2, 200, 4)

# COMMAND ----------

#Check Partitions
df_1.rdd.getNumPartitions()

# COMMAND ----------


#Repartition 
df_3=df_1.repartition(5)
df_4=df_2.repartition(7)

# COMMAND ----------

df_3.rdd.getNumPartitions()

# COMMAND ----------

#Join Dataframe
df_Join = df_3.join(df_4,on="id")

# COMMAND ----------

df_sum=df_Join.selectExpr("sum(id) as total_sum")

# COMMAND ----------

df_sum.show()

# COMMAND ----------

df_sum.explain()

# COMMAND ----------

df_union = df_sum.union(df_4)

# COMMAND ----------

df_union.show()

# COMMAND ----------

df_union.explain()

# COMMAND ----------


