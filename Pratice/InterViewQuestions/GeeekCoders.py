# Databricks notebook source
# MAGIC %md
# MAGIC ### Q1.Combining two Dataframes, No Same No of Columns

# COMMAND ----------

data1 = [(1,"Mohan",5000),(2,"Radha",6000)]
sch1=['id','ename','salary']

data2 = [(4,"Sumanth",10000,"Sales"),(3,"Siri",4000,"Marketing")]
sch2=['id','ename','salary','Department']

# COMMAND ----------

df1=spark.createDataFrame(data1,sch1)
df2=spark.createDataFrame(data2,sch2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SOlution 1 

# COMMAND ----------

df1.unionByName(df2,allowMissingColumns=True).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### SOlution 2

# COMMAND ----------

from pyspark.sql.functions import lit
df1 = df1.withColumn("Dept",lit(None))
df1.union(df2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2. Explode Columns using PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC #### Difference between explode() and explode_outer() 
# MAGIC used to transform array or map columns into multiple rows, 
# MAGIC
# MAGIC but there's an important difference in how they *** handle null or empty inputs**.
# MAGIC
# MAGIC explode_outer() will keep the null/None Values

# COMMAND ----------

from pyspark.sql.functions import explode,col
sample=[(1,["munna","singh"])
        ,(2,["Saurabh", "singh"])
        ,(3,["Dev", "Billu"])
        ,(4,["Sumit"])
        ]
column= ["id", "name"]        

df1= spark.createDataFrame(data=sample, schema=column)
#df1.withColumn("name",explode("name")).show()
# or 
df1.select(col("id"),explode(col("name")).alias("Name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3 Reg_exp to find correct Phno Number

# COMMAND ----------

from pyspark.sql.functions import col,rlike
datastu=[(1,"sagar","A23487V345"),(2,"Mohan","9566065421"),(3,"Radhar","123487V345")]
schstu=["id","name","phno"]
df=spark.createDataFrame(datastu,schstu)
#df.show()
df.filter(rlike(col("phno"),'^[0-9]*$')).show()

# COMMAND ----------

type(df)

# COMMAND ----------

df

# COMMAND ----------

from pyspark.sql.functions import col,rlike
datastu=[(1,"sagar","A234887V345"),(2,"Mohan","9566065421"),(3,"Radhar","123487V345")]
schstu=["id","name","phno"]
df1=spark.createDataFrame(datastu,schstu)
regexpression = '^[0-9]*$'
#df.show()
df1.filter(rlike(col("phno"),regexpression)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4 Count only Null record count from each column
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import count,when,col
data = [
    (1, "A", 23),
    (2, "B", None),
    (3, "C", 56),
    (4, None, None),
    (5, None, None)
]

data_schema=['ID','Name','Age']

df=spark.createDataFrame(data,data_schema)
df.select([count(when(col(i).isNull(),i)).alias(i) for i in df.columns]).show()

# COMMAND ----------

df1=df.select([(df.count()-count(i)).alias(i) for i in df.columns])
df1.show()

# COMMAND ----------

df.count()

# COMMAND ----------

df.select(count(col("Name"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4.Handle Muliple Delimeters
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import count,when,col,split
data = [
    (1, "A", '31|32|33'),
    (2, "B", '34|35|36'),
    (3, "C", '37|38|39'),
    (4, 'D', '40|41|43')
]

data_schema=['ID','Name','Age']

df=spark.createDataFrame(data,data_schema)
df.withColumns({"Maths":split(col("Age"),"\\|")[0],"Physics":split(col("Age"),"\\|")[1],"social":split(col("Age"),"\\|")[2]}).show()

# COMMAND ----------

x=3
y=5
a = lambda x,y:x*y
print(a(x,y))

# COMMAND ----------

students = [('Alice', 20), ('Bob', 22), ('Charlie', 19), ('David', 22)]
a = lambda s:s[1]
sorted_b = sorted(students,key=a,reverse=True)
print(sorted_b)

# COMMAND ----------

def get_age(student):
    return student[1]

print(get_age(students))

# COMMAND ----------


