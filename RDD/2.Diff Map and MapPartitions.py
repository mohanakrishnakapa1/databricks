# Databricks notebook source
# MAGIC %md
# MAGIC #########In PySpark, map() and mapPartitions() are two transformation operations that are used to apply a function to each element of an RDD in a distributed manner. However, there are some differences as follows:
# MAGIC
# MAGIC map()is a transformation operation that applies the specified function to each element of the RDD and returns a new RDD.
# MAGIC
# MAGIC The function passed to map() is applied individually to each element of the RDD.
# MAGIC
# MAGIC It operates on one element at a time and can be slower when the function has high overhead or requires external resources.
# MAGIC
# MAGIC mapPartitions()is a transformation operation that applies the specified function to each partition of the RDD and returns a new RDD.
# MAGIC
# MAGIC The function passed to mapPartitions() is applied to each partition as a whole, instead of individual elements.
# MAGIC
# MAGIC It can be more efficient when the function has a high overhead or requires external resources, as it reduces the overhead of function invocation by processing multiple elements at once.
# MAGIC
# MAGIC Here's a code example to illustrate the difference (firstly load and initiate sc instance):
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Create RDD:
rdd1 = sc.parallelize([7, 30, 36, 29, 20, 18, 9, 2, 23, 38], 4)
rdd1.collect()


# Define a function to double each element using map()
def double(x):
    return x * 2


# Apply map() transformation
map_result_rdd = rdd1.map(double)


# Define a function to double each element using mapPartitions()
def double_partition(partition):
    sum = 0
    for item in partition:
      sum = sum + (item * 2)
    yield sum


# Apply mapPartitions() transformation
mappartitions_result_rdd = rdd1.mapPartitions(double_partition)


# Print the results
print("RDD with partitions: ", rdd1.glom().collect())
print("Map Result: ", map_result_rdd.glom().collect())
print("MapPartitions Result: ", mappartitions_result_rdd.glom().collect())

# COMMAND ----------


