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
    ["018","104","Nancy Liu","29","Feale","50000","2017-06-01"],
    ["019","103","Steven Chen","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"


# COMMAND ----------

emp=spark.createDataFrame(emp_data,emp_schema)
from pyspark.sql.functions import when,col

# COMMAND ----------

#Creating a column using when
emp_Gender =emp.withColumn("newGender",when(col("gender")=="Male","M").when(col("gender")=="Female","F").otherwise(None))

# COMMAND ----------

#creating same with expr
from pyspark.sql.functions import expr
emp_gender_withexpr=emp.withColumn("newgender",expr("case when gender='Male' then 'M' when gender='Female' then 'F' else Null end "))

# COMMAND ----------

#reg_replace
from pyspark.sql.functions import regexp_replace
emp_Gender = emp_Gender.withColumn("new_name",regexp_replace(col("name"),'J','Z'))


# COMMAND ----------

emp_Gender.show(1)

# COMMAND ----------

#Convert Dates
from pyspark.sql.functions import to_date
emp_Gender =emp_Gender.withColumn('hire_date',to_date(col("hire_date"),"yyyy-MM-dd"))

# COMMAND ----------

emp_Gender.show(2)
emp_Gender.schema

# COMMAND ----------

#Add static Current_timestamp and current_date
from pyspark.sql.functions import current_timestamp,current_date
emp_Gender =emp_Gender.withColumn("currenttimestamp",current_timestamp()).withColumn("currentDate",current_date())

# COMMAND ----------

emp_Gender.show(1)

# COMMAND ----------

#to show all the data without Truncate
emp_Gender.show(truncate=False)

# COMMAND ----------

#Drop Null records
emp_Gender.na.drop().show()

# COMMAND ----------

#Remplace Null values
from pyspark.sql.functions import coalesce,lit
emp_Gender = emp_Gender.withColumn('newGender',coalesce("newGender",lit('O')))

# COMMAND ----------

emp_Gender.show()

# COMMAND ----------

columns_to_rename = {
    "new_name": "name",
    "newgender": "gender"
}

emp_final = emp_Gender.drop("name", "gender")
for old_col, new_col in columns_to_rename.items():
    emp_final = emp_final.withColumnRenamed(old_col, new_col)


# COMMAND ----------

emp_final.show()

# COMMAND ----------

#save the data
emp_final.write.format("csv").save("/FileStore/tables/pysparkfull/day3/empstringDateConversion.csv")


# COMMAND ----------

#Date format changes

from pyspark.sql.functions import date_format

emp_final.withColumn("daymonyear",date_format("hire_date","dd/MM/yyyy")).show()

# COMMAND ----------

#extract Yeat
emp_final.withColumn("year",date_format("currenttimestamp","yyyy")).show()

# COMMAND ----------

#Timezone
emp_final.withColumn("timezone",date_format("currenttimestamp","z")).show()

# COMMAND ----------

#hours
emp_final.withColumn("timezone",date_format("currenttimestamp","H")).show()

# COMMAND ----------


