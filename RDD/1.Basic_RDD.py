# Databricks notebook source
import random
ramdomlist=random.sample(range(0,40),10)

# COMMAND ----------

rdd=sc.parallelize(ramdomlist,4)

# COMMAND ----------

#To check where the Partitions are
rdd.glom().collect()

# COMMAND ----------

#print Spepcific partitio
rdd.glom().take(2)

# COMMAND ----------

#print last partition
rdd.glom().collect()[3]

# COMMAND ----------

#print count
rdd.count()

# COMMAND ----------

#first
rdd.first()

# COMMAND ----------

#top return the top 2 element based on sorting.
rdd.top(2)

# COMMAND ----------

def myfunc(item):
    return (item+1) * 3

# COMMAND ----------

#map() -- Transformation - Frequently used -- Retrun a new RDD.
rdd_map = rdd.map(myfunc)
rdd_map.collect()

# COMMAND ----------

rdd_map.glom().collect()

# COMMAND ----------

#analymous fun lambda
rdd_map1 = rdd.map(lambda item:(item+1)*3)
rdd_map1.collect()

# COMMAND ----------

#Filter
rdd_filter=rdd.filter(lambda x: x%3==0)
rdd_filter.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ###flatMap() -- Frequently Used one. When to Use flatMap: Aggregate the results we can use flatMap.

# COMMAND ----------

rdd_flatmap = rdd.flatMap(lambda x:[x+1,x+5])
rdd_flatmap.collect()

# COMMAND ----------

#using reduce
rdd_flatmap.reduce(lambda x,y:(x+y))

# COMMAND ----------

#descriptive Statitics
print([rdd.max(),rdd.min(),rdd.mean(),rdd.sum()])

# COMMAND ----------

#mapPartitions
def myfunc(item):
    sum=0
    for i in item:
        sum = sum + i
    yield sum

rdd.mapPartitions(myfunc).collect()


# COMMAND ----------


