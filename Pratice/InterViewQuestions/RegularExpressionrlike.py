# Databricks notebook source
spark

# COMMAND ----------

data = [
    (1,"john", "8203ge45939"),
    (2,"Alice", "9566065421"),
    (3,"Bob", "93582675634"),
    (4,"Mohana", "93h82675634")
   
]
schema = "id int,name string,phno string"
df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col
df.filter(col("phno").rlike("^[0-9]*$")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Same using spark sql

# COMMAND ----------

df.createOrReplaceTempView('student')

# COMMAND ----------

spark.sql("select * from student where phno rlike('^[0-9]*$')").show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC In Apache Spark, the rlike function is used for regular expression pattern matching, which is often more powerful than basic like matching because it allows you to use full regular expression syntax.
# MAGIC
# MAGIC Using rlike in Spark
# MAGIC The rlike function in Spark allows you to filter rows based on whether the values in a column match a regular expression pattern. It is available in DataFrame operations.
# MAGIC
# MAGIC Syntax:
# MAGIC python
# MAGIC Copy
# MAGIC df.filter(df['column_name'].rlike('regular_expression'))
# MAGIC Example:
# MAGIC Suppose we have a DataFrame df with a name column, and we want to filter rows based on regular expressions.
# MAGIC
# MAGIC python
# MAGIC Copy
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC # Start a Spark session
# MAGIC spark = SparkSession.builder.appName("RegexExample").getOrCreate()
# MAGIC
# MAGIC # Example data
# MAGIC data = [("Alice",), ("Bob",), ("Anna",), ("John",)]
# MAGIC df = spark.createDataFrame(data, ["name"])
# MAGIC
# MAGIC # Show all data
# MAGIC df.show()
# MAGIC Output:
# MAGIC
# MAGIC pgsql
# MAGIC Copy
# MAGIC +-----+
# MAGIC | name|
# MAGIC +-----+
# MAGIC |Alice|
# MAGIC |  Bob|
# MAGIC | Anna|
# MAGIC | John|
# MAGIC +-----+
# MAGIC Regular Expression Syntax in rlike
# MAGIC 1. ^ (Caret) - Start of the string
# MAGIC The ^ symbol is used to match the beginning of a string. If the regular expression starts with ^, it means the string must start with the specified pattern.
# MAGIC
# MAGIC python
# MAGIC Copy
# MAGIC # Filter names starting with "A"
# MAGIC df.filter(df['name'].rlike('^A')).show()
# MAGIC Output:
# MAGIC
# MAGIC pgsql
# MAGIC Copy
# MAGIC +-----+
# MAGIC | name|
# MAGIC +-----+
# MAGIC |Alice|
# MAGIC | Anna|
# MAGIC +-----+
# MAGIC ^A means the string should start with the letter "A".
# MAGIC 2. $ (Dollar sign) - End of the string
# MAGIC The $ symbol is used to match the end of a string. If the regular expression ends with $, it means the string must end with the specified pattern.
# MAGIC
# MAGIC python
# MAGIC Copy
# MAGIC # Filter names ending with "e"
# MAGIC df.filter(df['name'].rlike('e$')).show()
# MAGIC Output:
# MAGIC
# MAGIC pgsql
# MAGIC Copy
# MAGIC +-----+
# MAGIC | name|
# MAGIC +-----+
# MAGIC |Alice|
# MAGIC +-----+
# MAGIC e$ means the string should end with the letter "e".
# MAGIC 3. * (Asterisk) - Zero or more occurrences of the preceding character
# MAGIC The * symbol matches zero or more occurrences of the character or group preceding it. It is useful when you want to match repeating patterns or allow for flexibility in the length of the pattern.
# MAGIC
# MAGIC python
# MAGIC Copy
# MAGIC # Filter names containing "a" followed by any number of characters
# MAGIC df.filter(df['name'].rlike('a*')).show()
# MAGIC Output:
# MAGIC
# MAGIC pgsql
# MAGIC Copy
# MAGIC +-----+
# MAGIC | name|
# MAGIC +-----+
# MAGIC |Alice|
# MAGIC | Anna|
# MAGIC +-----+
# MAGIC a* means "zero or more occurrences of 'a'". This will match names that have no 'a' at all or any number of 'a's in them.
# MAGIC 4. Combining ^, $, and *
# MAGIC You can combine these symbols to create more complex patterns.
# MAGIC
# MAGIC python
# MAGIC Copy
# MAGIC # Filter names that start with "A" and have zero or more "n"s after it
# MAGIC df.filter(df['name'].rlike('^A*n$')).show()
# MAGIC Output:
# MAGIC
# MAGIC pgsql
# MAGIC Copy
# MAGIC +-----+
# MAGIC | name|
# MAGIC +-----+
# MAGIC |Alice|
# MAGIC | Anna|
# MAGIC +-----+
# MAGIC ^A*n$ means the string must start with an "A", followed by zero or more "n"s, and end with "n".
# MAGIC Additional Common Regular Expressions in Spark's rlike
# MAGIC . (dot) - Matches any single character except line breaks.
# MAGIC [] (square brackets) - Matches any single character within the brackets.
# MAGIC | (pipe) - Logical OR for matching one of several patterns.
# MAGIC () (parentheses) - Groups parts of the pattern.
# MAGIC + (plus) - Matches one or more occurrences of the preceding character.
# MAGIC ? (question mark) - Matches zero or one occurrence of the preceding character.
# MAGIC Example of More Complex Regular Expressions
# MAGIC Example 1: Matching any string that contains "o" or "a" followed by "b"
# MAGIC python
# MAGIC Copy
# MAGIC df.filter(df['name'].rlike('o|a.*b')).show()
# MAGIC Example 2: Matching names that contain at least one "o", followed by zero or more letters
# MAGIC python
# MAGIC Copy
# MAGIC df.filter(df['name'].rlike('o[a-zA-Z]*')).show()
# MAGIC Summary:
# MAGIC ^: Anchors the pattern to the start of the string.
# MAGIC $: Anchors the pattern to the end of the string.
# MAGIC *: Matches zero or more occurrences of the preceding character or group.
# MAGIC rlike: Allows you to perform regular expression matching in Spark DataFrames, which is more flexible than the like operator.

# COMMAND ----------


