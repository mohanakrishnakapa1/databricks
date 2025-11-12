# Databricks notebook source
rdd=sc.textFile("/FileStore/tables/kddcup_data.gz")

# COMMAND ----------

rdd.count()

# COMMAND ----------

rdd.take(5)

# COMMAND ----------

rdd_map = rdd.map(lambda x:x.split(','))

# COMMAND ----------

rdd_map.take(2)[0][1:]

# COMMAND ----------

#Get the ten records randomly
rdd.takeSample(False,10,1234)

# COMMAND ----------

#Get normal ratio
Normal_rdd=rdd.filter(lambda x:'normal.' in x)
ratio=Normal_rdd.count()/rdd.count()
ratio

# COMMAND ----------

#Get the list of Lables. (get the last element)
rdd_map.map(lambda x:x[-1]).distinct().take(20)

# COMMAND ----------

rdd_map.filter(lambda line:line[-1]).collect()

# COMMAND ----------


