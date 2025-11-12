# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, regexp_extract

spark = SparkSession.builder.appName("RegexExample").getOrCreate()

data = [("John Doe",), ("Jane Smith",), ("Bob Johnson",)]
df = spark.createDataFrame(data, ["text"])

# Using regexp_replace to replace "Doe" with "Smith" in the "text" column
df = df.withColumn("replaced_text", regexp_replace(df["text"], "Doe", "Smith"))

# Using regexp_extract to extract the first name from the "text" column
df = df.withColumn("first_name", regexp_extract(df["text"], r'(\w+)', 0))

df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #############Special Characters in Regular Expressions:
# MAGIC *: The asterisk (*) is a quantifier that matches zero or more occurrences of the preceding character or group. For example, the pattern a* would match zero or more 'a' characters.
# MAGIC
# MAGIC ^: The caret (^) is an anchor used at the beginning of a regular expression to match the start of a string. For example, ^abc would match "abc" at the start of the string.
# MAGIC
# MAGIC $: The dollar sign ($) is an anchor used at the end of a regular expression to match the end of a string. For example, xyz$ would match "xyz" at the end of the string.
# MAGIC
# MAGIC For example, if you want to find all strings that start with "abc" and end with "xyz" in a column, you can use a regular expression like this: ^abc.*xyz$. This pattern matches strings that start with "abc" and end with "xyz," with any characters in between.

# COMMAND ----------

# MAGIC %md
# MAGIC ####using *

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName("RegexExample").getOrCreate()

data = [("abc123",), ("123abc",), ("xyzabc",), ("abcxyz",)]
df = spark.createDataFrame(data, ["name"])

# Using a regular expression to match zero or more 'a' characters
df = df.withColumn("matches", regexp_extract(df["name"], r'a*', 0))

df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ####using $

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName("RegexExample").getOrCreate()
data = [("abc123",), ("123abc",), ("xyzabc",), ("abcxyz",)]
df = spark.createDataFrame(data, ["text"])

# Using a regular expression to match "xyz" at the end of the string
df = df.withColumn("matches", regexp_extract(df["text"], r'xyz$', 0))

df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #############using ^

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName("RegexExample").getOrCreate()

data = [("abc123",), ("123abc",), ("xyzabc",), ("abcxyz",)]
df = spark.createDataFrame(data, ["text"])

# Using a regular expression to match "abc" at the start of the string
df = df.withColumn("matches", regexp_extract(df["text"], r'^abc', 0))

df.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName("RegexExample").getOrCreate()

data = [("abc123",), ("123abc",), ("xyzabc",), ("abcxyz",)]
df = spark.createDataFrame(data, ["name"])

# Using a regular expression to match zero or more 'a' characters
df = df.withColumn("matches", regexp_extract(df["name"], r'^[abc]*$', 0))

df.show()


# COMMAND ----------


