# Databricks notebook source
# MAGIC %md
# MAGIC ##########Add File Name to All reading files

# COMMAND ----------

from pyspark.sql.functions import input_file_name
df=spark.read.csv("/FileStore/tables/emp/.*csv",header=True)
df_final=df.withCoumn('file_name',input_file_name())


# COMMAND ----------

# MAGIC %md
# MAGIC ##########Get the count of records that are going to load

# COMMAND ----------

df.select('file_name').groupBy("file_name").count()

# COMMAND ----------

# MAGIC %md
# MAGIC #############Check the Partitions and Partition ID

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id
from pyspark.sql.functions import input_file_name
df=spark.read.csv("/FileStore/tables/emp/*.csv",header=True)
df_final=df.withCoumn('partition_id',spark_partition_id())


# COMMAND ----------

#to Check Count
df.select('partition_id').groupBy("partition_id").count()

# COMMAND ----------

data = [
    ("john", "tomato", 2),
    ("𝚋𝚒𝚕𝚕", "𝚊𝚙𝚙𝚕𝚎", 2),
    ("john", "𝚋𝚊𝚗𝚊𝚗𝚊", 2),
    ("john", "tomato", 3),
    ("𝚋𝚒𝚕𝚕", "𝚝𝚊𝚌𝚘", 2),
    ("𝚋𝚒𝚕𝚕", "𝚊𝚙𝚙𝚕𝚎", 2),
]
schema = "name string,item string,weight int"
df_new = spark.createDataFrame(data, schema)


# COMMAND ----------

df_new.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ############### It will generate based on primary key or multiple columns . Its unique

# COMMAND ----------

from pyspark.sql.functions import sha2
df_new.withColumn("key",sha2("weight",256)).show()

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws

# Assuming 'column1' and 'column2' are the two columns for which you want to generate a hash key
df_new = df_new.withColumn("key_combined", sha2(concat_ws("-", "weight", "item"), 256))

df_new.show()


# COMMAND ----------

#We can change this configuration.
spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### this will create 8 partitions by default for sc.parallelize method. But for sc.textFile() it will create two partitions

# COMMAND ----------

rdd=sc.parallelize([1,2,3,4,5,6,7,8])
rdd.getNumPartitions()


# COMMAND ----------

sc.defaultMinPartitions

# COMMAND ----------

sc.defaultParallelism

# COMMAND ----------

rdd1=sc.textFile("/FileStore/tables/2021-03-21/circuits.csv")

# COMMAND ----------

rdd1.getNumPartitions()

# COMMAND ----------

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Path Global Filter
# MAGIC #########To read only include files with file names matching pattern

# COMMAND ----------

from pyspark.sql.functions import *
df=spark.read.format("csv").option("pathGlobFilter","employees*.csv").load("/FileStore/tables/").withColumn("file_name",input_file_name())
display(df.select("file_name").distinct())

# COMMAND ----------

from pyspark.sql.functions import *
df=spark.read.format("csv").option("pathGlobFilter","*.csv").load("/FileStore/tables/").withColumn("file_name",input_file_name())
display(df.select("file_name").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ####recusriveFileLookup to check files recursively and load file.
# MAGIC
# MAGIC ################"recusriveFileLookup","*.csv" or "recusriveFileLookup","true" if you are using spark.read.scv()

# COMMAND ----------

from pyspark.sql.functions import *
df=spark.read.format("csv").option("recusriveFileLookup","*.csv").load("/FileStore/tables/").withColumn("file_name",input_file_name())
display(df.select("file_name").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ### WE can read like this as well

# COMMAND ----------

df=spark.read.option("header",True).csv(["/FileStore/tables/*.csv,/FileStore/tables/2021-03-01*.csv,/FileStore/tables/2021-04-01*.csv"])

# COMMAND ----------

df=spark.read.option("header",True).csv(["/FileStore/tables/Data[1-3]*/*.csv"])

# COMMAND ----------

from pyspark.sql.functions import *
f1=spark.sql("show functions")
f1.count()
f1.show()

# COMMAND ----------

f1.filter("function like 'co%'").show()

# COMMAND ----------

spark.sql('describe function collect_list').collect()

# COMMAND ----------

df=spark.read.csv("/FileStore/tables/2021-03-21/circuits.csv",inferSchema=True,header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

#filter
df.filter("country='UK' and location='Liverpool'").show()

# COMMAND ----------

#filter
from pyspark.sql.functions import *
df.filter((col("country")=='UK') & (col("location")=='Liverpool')).show()

# COMMAND ----------

#filter
from pyspark.sql.functions import *
df.filter((col("country")=='UK') | (col("location")=='Liverpool')).show()

# COMMAND ----------

#group by
df.groupby('country').count().show()

# COMMAND ----------

df.show(2)

# COMMAND ----------

#group by Multiple cols
df.groupby("country").agg(sum("alt"),count("name"),max("alt")).show()

# COMMAND ----------

#group by Multiple cols
df.groupby("country","location").agg(sum("alt"),count("name"),max("alt")).show()

# COMMAND ----------

df_uk=df.filter("country='UK' or country='USA'")

# COMMAND ----------

df_uk.show()

# COMMAND ----------

#collect_list and collect_set
df.groupby("country").agg(collect_list("location").alias("List_location")).show()


# COMMAND ----------

#collect_list and collect_set with dual aggregation columns
df.groupby("country").agg(collect_set("location").alias("List_location"),sum("alt").alias("alt_sum")).show()

# COMMAND ----------

df_uk.show(2)

# COMMAND ----------

#window Funtions and order by
w=Window.partitionby("location").orderby("alt").desc()


# COMMAND ----------


