# Databricks notebook source
emp_data = [
    ["001","101","John Doe","30","Male","50000","2015-01-01"],
    ["002","101","Jane Smith","25","Female","45000","2016-02-15"],
    ["003","102","Bob Brown","35","Male","55000","2014-05-01"],
    ["004","102","Alice Lee","28","Female","48000","2017-09-30"],
    ["005","103","Jack Chan","40","Male","60000","2013-04-01"],
    ["006","103","Jill Wong","32","Female","52000","2018-07-01"],
    ["007","101","James Johnson","42","Male","70000","2012-03-15"],
    ["008","102","Kate Kim","29","Female","51000","2019-10-01"],
    ["009","103","Tom Tan","33","Male","58000","2016-06-01"],
    ["010","104","Lisa Lee","27","Female","47000","2018-08-01"],
    ["011","104","David Park","38","Male","65000","2015-11-01"],
    ["012","105","Susan Chen","31","Female","54000","2017-02-15"],
    ["013","106","Brian Kim","45","Male","75000","2011-07-01"],
    ["014","107","Emily Lee","26","Female","46000","2019-01-01"],
    ["015","106","Michael Lee","37","Male","63000","2014-09-30"],
    ["016","107","Kelly Zhang","30","Female","49000","2018-04-01"],
    ["017","105","George Wang","34","Male","57000","2016-03-15"],
    ["018","104","Nancy Liu","29","Female","50000","2017-06-01"],
    ["019","103","Steven Chen","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"

# COMMAND ----------

# Create emp DataFrame

emp = spark.createDataFrame(data=emp_data, schema=emp_schema)

# COMMAND ----------

emp.show()

# COMMAND ----------

emp.select("salary").show()

# COMMAND ----------

#select the Columns in different ways in pyspark
#import sql Functions
from pyspark.sql.functions import expr,col



# COMMAND ----------

#As this is Transformation Nothing will happen
emp_filtered = emp.select(col("employee_id"),expr("name"),emp.age,emp.salary)

# COMMAND ----------

#Action will happen once we select the Data.

emp_filtered.show()

# COMMAND ----------

#USe expr to Cast and rename the columns in Spark
emp_changed = emp_filtered.select(expr("employee_id as emp_id"),col("name"),expr("cast(age as int) as age "),col("salary"))

# COMMAND ----------

emp_changed.show()
emp_changed.schema

# COMMAND ----------

#all these operations using selectExpr

emp_changed_1 = emp_filtered.selectExpr("employee_id as emp_id","name","cast(age as int)","salary")
emp_changed_1.show()
emp_changed_1.schema

# COMMAND ----------

#using where to filter the data
emp_where_age_greater = emp_changed_1.select("emp_id","name","age","salary").where("age>30")
emp_where_age_greater.show()

# COMMAND ----------

emp_where_age_greater.write.format("csv").save("/FileStore/tables/pysparkfull/day1/empfilter.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Bonus Tip of the Day
# MAGIC how Spark is Indentifying even we have defining the schema like below
# MAGIC schema_str=["emp_id str","name str","age int"]
# MAGIC Spark uses an implicit conversion method for this which will show in next cell

# COMMAND ----------

schema_str="emp_id string,name string,age int"
from pyspark.sql.types import _parse_datatype_string
schema_spark = _parse_datatype_string(schema_str)
schema_spark

# COMMAND ----------

#another way to define structtype schema using StructFields

from pyspark.sql.types import StructType, StructField, StringType

emp_schema = StructType([
    StructField("employee_id", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("age", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", StringType(), True),
    StructField("hire_date", StringType(), True)
])

# COMMAND ----------


