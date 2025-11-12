# Databricks notebook source
#union
rdd=sc.parallelize([5,12,64,23,54,23,7,4,2,74,23],4)
rdd1=sc.parallelize([1,5,2,75,34,2,3,5,12,7],2)
rdd_union=rdd.union(rdd1)
rdd_union.collect()


# COMMAND ----------

#check partitions
print(rdd_union.getNumPartitions())

# COMMAND ----------

#intersectio
rdd_intersection=rdd.intersection(rdd1)
rdd_intersection.collect()

# COMMAND ----------

#to check num of empty partitions
rdd_intersection.glom().collect()

# COMMAND ----------

# to find the empty parition
counter=0
for em in rdd_intersection.glom().collect():
    if len(em)==0:
        counter = counter +1

print(counter)



# COMMAND ----------

#coalsce
rdd_intersection.coalesce(1).glom().collect()


# COMMAND ----------

rdd.collect()

# COMMAND ----------

#take sample
rdd.takeSample(False,5)

# COMMAND ----------

#takeOrdered(n,[Ordering])
print(rdd.takeOrdered(4))
print(rdd.takeOrdered(5,lambda x : -x))

# COMMAND ----------

#reduce (Aggregate Results)
print(rdd.collect())
print(rdd.reduce(lambda x,y:x*y))
print(rdd.reduce(lambda x,y:x+y))

# COMMAND ----------

#reducebykey(Aggregate results based on key)
rdd_rbk=sc.parallelize([(1,4),(7,10),(5,7),(1,12),(7,12),(7,1),(9,1),(7,4),(9,8),(2,5),(9,18),(2,6),(3,4)],2)
print(rdd_rbk.collect())
rdd_rbk.reduceByKey(lambda x,y:x+y).collect()

#user frinedly Visualization
import pandas as pd
count = pd.DataFrame({'key':rdd_rbk.keys().collect(),'values':rdd_rbk.values().collect()})
count

# COMMAND ----------

#SortBy Key
print(rdd_rbk.reduceByKey(lambda x,y:x+y).sortByKey().collect())
print(rdd_rbk.reduceByKey(lambda x,y:x+y).sortByKey(False).collect())



# COMMAND ----------

#CountByKey
print(rdd_rbk.countByKey())
print(rdd_rbk.countByKey().items())
print(sorted(rdd_rbk.countByKey()))

# COMMAND ----------

#groupByKey.Its is going to collect values and send it Driver. It will be Costly operations It is not good for Huge Size.
rdd_group=rdd_rbk.groupByKey()
rdd_group.getNumPartitions()
for item in rdd_group.collect():
    print(item[0],[values for values in item[1]])

# COMMAND ----------

#lookup(key): for specific key
rdd_rbk.lookup(9)


# COMMAND ----------

# MAGIC %md
# MAGIC ######cache & persist. Spark Garbage collector remove RDD if they are not being used. Better use cache or persist if you are calling RDD freqeuntly

# COMMAND ----------

rdd.cache()


# COMMAND ----------

from pyspark import StorageLevel
rdd.persist(StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------


