# Databricks notebook source
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("AddressDataFrame").getOrCreate()

# Define the data as a list of tuples
data = [
    ("93 NORTH 9TH STREET, BROOKLYN NY 11211", "NORTH TH STREET BROOKLYN NY"),
    ("380 WESTMINSTER ST, PROVIDENCE RI 02903", "WESTMINSTER ST PROVIDENCE RI"),
    ("177 MAIN STREET, LITTLETON NH 03561", "MAIN STREET LITTLETON NH"),
    ("202 HARLOW ST, BANGOR ME 04401", "HARLOW ST BANGOR ME"),
    ("46 FRONT STREET, WATERVILLE, ME 04901", "FRONT STREET WATERVILLE ME"),
    ("22 SUSSEX ST, HACKENSACK NJ 07601", "SUSSEX ST HACKENSACK NJ"),
    ("75 OAK STREET, PATCHOGUE NY 11772", "OAK STREET PATCHOGUE NY"),
    ("1 CLINTON AVE, ALBANY NY 12207", "CLINTON AVE ALBANY NY"),
    ("7242 ROUTE 9, PLATTSBURGH NY 12901", "ROUTE PLATTSBURGH NY"),
    ("520 5TH AVE, MCKEESPORT PA 15132", "TH AVE MCKEESPORT PA"),
    ("122 W 3RD STREET, GREENSBURG PA 15601", "W RD STREET GREENSBURG PA")
]

# Define the column names
columns = ["address", "clean_address"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ###################In the regular expression [^A-Za-z\s], the ^ and \s have specific meanings:
# MAGIC
# MAGIC ^: The caret (^) character, when placed at the beginning of a character class (inside square brackets []), negates the class. It means that the regular expression will match any character that is not in the specified character class. In this case, it's used to match any character that is not an uppercase letter (A-Z), a lowercase letter (a-z), or whitespace.
# MAGIC
# MAGIC \s: The \s is an escape sequence used to represent whitespace characters in regular expressions. It matches any whitespace character, including spaces, tabs, and newline characters. In this regular expression, it's part of the character class, meaning it will match any character that is not a whitespace character.
# MAGIC
# MAGIC So, the regular expression [^A-Za-z\s] will match any character that is not an uppercase letter, a lowercase letter, or a whitespace character.

# COMMAND ----------

from pyspark.sql.functions import regexp_extract,regexp_replace,col,reverse,repeat
df.withColumn("New_Address",regexp_replace(col("address"),'[^A-Za-z\s]','')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Extract only Number With Spaces

# COMMAND ----------

from pyspark.sql.functions import regexp_extract,regexp_replace,col,reverse,repeat
df.withColumn("New_Address",regexp_replace(col("address"),'[^0-9\s]','')).show()




# COMMAND ----------

# MAGIC %md
# MAGIC ####Extract Numbers Without Space remove \s

# COMMAND ----------

from pyspark.sql.functions import regexp_extract,regexp_replace,col,reverse,repeat
df.withColumn("New_Address",regexp_replace(col("address"),r'[^0-9]','')).show()

# COMMAND ----------

df.filter(col("address").rlike("11211$")).show()

# COMMAND ----------

df.filter(col("address").rlike(".*STREET.*")).show()

# COMMAND ----------

df.filter(col("address").rlike("STREET*")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #################regexp_extract — Extract a specific group matched by a Java regex, from the specified string column. If the regex did not match, or the specified group did not match, an empty string is returned.
# MAGIC
# MAGIC pyspark.sql.functions.regexp_extract(str, pattern, idx)
# MAGIC
# MAGIC It takes 3 arguments
# MAGIC 1. str — a column where you want to perform a regexp match.
# MAGIC 2. pattern — regexp pattern to extract substring from the specified string column
# MAGIC 3. idx — part of the match we need to extract from the group of match
# MAGIC
# MAGIC (\d*) — Match 0 or more digits (0–9)
# MAGIC Below we extracted number, city, state, zip_code from the address using regexp_extract.
# MAGIC Here we are creating groups pattern and based on need we are selecting a specific group by providing idx.

# COMMAND ----------

#Extract only Digits 1 Group
df.withColumn("Extract_Hours_No",regexp_extract(col("address"),"(\d*)",0))\
    .withColumn("city",regexp_extract(col("address"),"(,) (\w*)",2))\
    .withColumn("state",regexp_extract(col("address"),"(\w*) (\w*$)",1))\
    .withColumn("state1",regexp_extract(col("address"),"(\w*) (\w*)",0))\
    .withColumn("state2",regexp_extract(col("address"),"(\w*$)",0))\
    .withColumn("zip_code",regexp_extract(col("address"), "(\d*$)", 1))\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ########In regular expressions used in functions like regexp_extract, there are various special characters and symbols that have specific meanings. Here are some commonly used special characters and their meanings:
# MAGIC
# MAGIC . (dot): Matches any single character except a newline.
# MAGIC
# MAGIC *: Matches the preceding character or subexpression zero or more times.
# MAGIC
# MAGIC +: Matches the preceding character or subexpression one or more times.
# MAGIC
# MAGIC ?: Matches the preceding character or subexpression zero or one time (optional).
# MAGIC
# MAGIC | (pipe): Acts as an OR operator, allowing you to match one of multiple expressions. For example, A|B matches either "A" or "B."
# MAGIC
# MAGIC () (parentheses): Used for grouping subexpressions and capturing groups. Capturing groups allow you to extract specific portions of the matched text.
# MAGIC
# MAGIC [] (square brackets): Used to define character classes. For example, [0-9] matches any single digit.
# MAGIC
# MAGIC [^] (caret inside square brackets): Matches any character not in the specified character class. For example, [^0-9] matches any character that is not a digit.
# MAGIC
# MAGIC {} (curly braces): Specifies a range for repetition. For example, a{3} matches exactly three consecutive "a" characters.
# MAGIC
# MAGIC \: Used to escape special characters to match them literally. For example, \$ matches the dollar sign character, and \\ matches the backslash character.
# MAGIC
# MAGIC ^ (caret) and $ (dollar sign): Match the start and end of a line or string, respectively.
# MAGIC
# MAGIC \b: Matches a word boundary. For example, \bword\b matches the word "word" as a whole word and not as part of another word.
# MAGIC
# MAGIC \d, \w, \s: Represent shorthand character classes for digits, word characters (letters, digits, and underscores), and whitespace characters, respectively.
# MAGIC
# MAGIC [^...]: Negates a character class, matching any character not listed within the brackets.

# COMMAND ----------

# Given DataFrame `df` with "address" column

# Example 1: Extract the street number (digits at the beginning)
df = df.withColumn("street_number", regexp_extract(df["address"], r'^\d+', 0))

# Example 2: Extract the state abbreviation (two uppercase letters at the end)
df = df.withColumn("state", regexp_extract(df["address"], r' (,) ([A-Z]{2})', 1))

# Example 3: Extract the city name (all text after the first comma)
df = df.withColumn("city", regexp_extract(df["address"], r', (.+)$', 1))

# Show the DataFrame with the extracted information
df.show(4)


# COMMAND ----------

# MAGIC %md
# MAGIC #####Trying to Extract only State

# COMMAND ----------

df.withColumn("state_new", regexp_extract(df["address"], r'(,.*) ([A-Z]{2})', 2)).show()

# COMMAND ----------


