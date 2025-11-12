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

from pyspark.sql.functions import col
emp=spark.createDataFrame(emp_data,emp_schema)

# COMMAND ----------

#cast a column in spark
emp_selected = emp.select("employee_id","name","age",col("salary").cast("double"))

# COMMAND ----------

emp_selected.show(1)
emp_selected.schema

# COMMAND ----------

#Rename a column and add a static column using lit 
from pyspark.sql.functions import lit
emp_selected = emp_selected.withColumn("columnone",lit(1)).withColumn("columnTwo",lit("Two"))
emp_selected = emp_selected.withColumnRenamed("empoloyee_id","emp_id")


# COMMAND ----------

emp_selected.show(1)

# COMMAND ----------

emp_dropped = emp_selected.drop("columTwo")
#Drop two to three columns
emp_dropped = emp_selected.drop("columTwo","columnone")

# COMMAND ----------

emp_dropped.show(1)

# COMMAND ----------

#add Tax column column
emp_selected = emp_selected.withColumn("tax",col("salary")*0.2)

# COMMAND ----------

emp_selected.show(1)

# COMMAND ----------

#Tax greater than 3000
emp_tax_grear=emp_selected.where("tax > 10000")

# COMMAND ----------

emp_tax_grear.show()

# COMMAND ----------

#Using limit
emp_tax_grear.limit(3).show()

# COMMAND ----------

#Add three columns at a time
columns={
  "tax":col("salary")*2,
  "columnone":lit(1),
  "columntwo":lit("two")

}
emp_addallthecolumnsatonce = emp.withColumns(columns)

# COMMAND ----------

emp_addallthecolumnsatonce.show(2)

# COMMAND ----------

#anotherway
emp_addallthecolumnsatonce_1=emp.withColumns( "tax",col("salary")*2,
  "columnone",lit(1),
  "columntwo",lit("two"))

# COMMAND ----------


