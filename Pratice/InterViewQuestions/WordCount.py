# Databricks notebook source
data = [
    "Hello Spark, this is a word count example.",
    "Spark is powerful, Spark is fun.",
    "Count words in this example.",
    "This is another line."
]

# COMMAND ----------

rdd_data=spark.sparkContext.parallelize(data)
print(rdd_data.collect())

# COMMAND ----------

wrd_data = rdd_data.flatMap(lambda wrd:wrd.split(" "))
print(wrd_data.collect())

# COMMAND ----------

map_rdd = wrd_data.map(lambda wrd:(wrd,1))

# Reduce by key would need lambda as its takes previous and current values from another word. so a+b in the sense hello,1 and hello,1 at another node. so a=1 and b=1
final_lst= map_rdd.reduceByKey(lambda x,b:x+b)
final_lst.sortBy(lambda x:x[1],ascending=False).collect()[0][1]

# COMMAND ----------


