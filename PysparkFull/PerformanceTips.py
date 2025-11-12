# Databricks notebook source
# MAGIC %md
# MAGIC 1.UDF Drawback and Tip

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario: Although UDFs are powerful, they come with a performance penalty compared to using built-in Spark functions. UDFs can be slower because they involve serialization and deserialization between JVM (Java) and Python processes in PySpark. Therefore, it's always recommended to use Spark's built-in functions whenever possible. If you use UDFs, it should be when performance trade-offs are acceptable or necessary.
# MAGIC
# MAGIC Tip: Spark provides optimized alternatives to UDFs through pandas_udf (in PySpark) or pandas_udf (in Scala) to handle distributed data in a vectorized manner, which can be faster than regular UDFs.

# COMMAND ----------

# MAGIC %md
# MAGIC The next program there is column Score extraction from API data, So we should use UDF to extract that Information.
# MAGIC
# MAGIC import requests
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql.functions import udf
# MAGIC from pyspark.sql.types import BooleanType
# MAGIC
# MAGIC # Initialize Spark session
# MAGIC spark = SparkSession.builder.appName("FraudDetectionUDF").getOrCreate()
# MAGIC
# MAGIC # Sample transaction data
# MAGIC data = [
# MAGIC     ("John", 100, "USA", "China", 500),   # (name, amount, user_location, ip_location, transaction_amount)
# MAGIC     ("Alice", 200, "USA", "USA", 1500),
# MAGIC     ("Bob", 50, "Canada", "Canada", 600),
# MAGIC     ("Charlie", 300, "UK", "UK", 2000)
# MAGIC ]
# MAGIC
# MAGIC columns = ["name", "user_location", "ip_location", "avg_transaction", "transaction_amount"]
# MAGIC
# MAGIC # Create DataFrame
# MAGIC df = spark.createDataFrame(data, columns)
# MAGIC
# MAGIC # UDF to check if the transaction is from a suspicious location
# MAGIC def is_suspicious_location(user_location, ip_location):
# MAGIC     return user_location != ip_location
# MAGIC
# MAGIC # Register UDF for suspicious location check
# MAGIC location_check_udf = udf(is_suspicious_location, BooleanType())
# MAGIC
# MAGIC # UDF to call an external API for credit score validation
# MAGIC def get_credit_score(user_name):
# MAGIC     response = requests.get(f"https://api.creditscore.com/{user_name}")
# MAGIC     score = response.json()['score']
# MAGIC     return score
# MAGIC
# MAGIC # Register UDF for external credit score API
# MAGIC credit_score_udf = udf(get_credit_score, IntegerType())
# MAGIC
# MAGIC # Apply UDFs
# MAGIC df_with_flags = df \
# MAGIC     .withColumn("location_suspicious", location_check_udf(df["user_location"], df["ip_location"])) \
# MAGIC     .withColumn("credit_score", credit_score_udf(df["name"])) \
# MAGIC     .withColumn("transaction_suspicious", df["transaction_amount"] > (df["avg_transaction"] * 3))
# MAGIC
# MAGIC df_with_flags.show()
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##   ************************Check DAG and Shuffle Notebooks******************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Shuffle Notebook ************

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check Cache and Persist NoteBook******

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check BroadCast and Accumulators NoteBook ****
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Join Notebook to Understand to optmize joins using paritioning and Bucketing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check FiX Skewness Notebook very important **

# COMMAND ----------

# MAGIC %md
# MAGIC ## check Static vs Dynmaic Resource Notebook Not that Important

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Skewness and Spliage  NoteBook ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check AQE Which is very important, Introduced in Spark 3.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Spark Hints Which is very important while using Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC
