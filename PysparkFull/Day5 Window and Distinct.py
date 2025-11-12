# Databricks notebook source
# Emp Data & Schema

emp_data_1 = [
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
    ["018","104","Nancy Liu","29","","50000","2017-06-01"],
    ["019","103","Steven Chen","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"

# COMMAND ----------

emp = spark.createDataFrame(emp_data_1,emp_schema)

# COMMAND ----------

#distinct data
emp_unique = emp.distinct()

# COMMAND ----------

#select only Distinct Depatment IDs
depatments_distinct = emp.select("department_id").distinct()

# COMMAND ----------

depatments_distinct.show()

# COMMAND ----------

from pyspark.sql.functions import col,max,desc,row_number
from pyspark.sql.window import Window

window_spec = Window.partitionBy(col("department_id")).orderBy(col("salary").desc())
max_spec = max(col("salary")).over(window_spec)

emp1 = emp.withColumn("MaxSalByDepartment",max_spec).show()

# COMMAND ----------

#Find the Second Highest Salary using row_number
rn = row_number().over(window_spec)
emp_secondhighestSalary = emp.withColumn("rowNum",rn).filter("rowNum == 2")

# COMMAND ----------

emp_secondhighestSalary.show()

# COMMAND ----------

#same with Expr
from pyspark.sql.functions import expr

emp_secondsalartbydepat = emp.withColumn("rownum",expr("row_number () over(partition by department_id order by salary desc)")).where(col("rownum")==2)

# COMMAND ----------

emp_secondsalartbydepat.show()

# COMMAND ----------


