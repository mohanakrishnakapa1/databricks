# Databricks notebook source
spark.sql(r"select regexp_extract('123','(\d+)',1)").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ############As of now we will assume this function will extract digit but since this string will be converted to java column and backslash have a special meaning in java we need to escape it with another backslash as shown below

# COMMAND ----------

spark.sql(r"select regexp_extract('123','(\\d+)',1)").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Extract only Digit from the below string 11ss

# COMMAND ----------

spark.sql(r"select regexp_extract('11ss','(\\d+)',1)").show()

# COMMAND ----------



# COMMAND ----------

spark.sql(r"select regexp_extract('11ss','(\\d+)(\\w+)',2)").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###############
# MAGIC Capture is concept in regex expression where we need to use the captured data in regexp_replace with replace part.we can access the captured group by using dollar sign($) and it will start from index zero to no of brackets
# MAGIC
# MAGIC For example we have used 2 brackets and when we replace with $0(red) it will use the whole group and $1 indicate first bracket(yellow) and $2 can be used for second group(green)

# COMMAND ----------

spark.sql(r"select regexp_replace('11ss', '(\\d+)(\\w+)', '$0')").show()


# COMMAND ----------

spark.sql(r"select regexp_replace('11ss', '(\\d+)(\\w+)', '$1')").show()


# COMMAND ----------

spark.sql(r"select regexp_replace('11ss', '(\\d+)(\\w+)', '$2')").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #############for example a common use case is to mock sensitive data like card with x
# MAGIC
# MAGIC so we are going to hide the digits alone from below string for security reason

# COMMAND ----------

spark.sql(r"select regexp_replace('aaaassss11ss', '([a-z]+)(\\d+)([a-z]+)', '$1X$3')").show()

# COMMAND ----------

spark.sql(r"select regexp_extract('11ss','(\w.)',0)").show()

# COMMAND ----------

spark.sql(r"select regexp_extract('11ss', '([a-z$])', 1)").show()

# COMMAND ----------

spark.sql(r"select regexp_extract('11ss', '([a-z$])', 1)").show()


# COMMAND ----------


