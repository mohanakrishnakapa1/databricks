# Databricks notebook source

from pyspark.sql.types import StructType,StructField,StringType,IntegerType
schema = StructType([
    StructField("player", StringType(), True),
    StructField("runs", IntegerType(), True),
    StructField("50s/100s", StringType(), True)
])

data = [("Sachin-IND", 18694, "93/49"), ("Ricky-AUS", 11274, "66/31"),("Lara-WI", 10222, "45/21"),("Rahul-IND", 10355, "95/11"),("Jhonty-SA", 7051, "43/5"),("Hayden-AUS", 8722, "67/19")]
players_df = spark.createDataFrame(data, schema)

data1 = [("IND", "India"), ("AUS", "Australia"), ("WI", "WestIndies"), ("SA", "SouthAfrica")]
countries_df = spark.createDataFrame(data1,["SRT","country"])

# COMMAND ----------


from pyspark.sql.functions import split,col
PlyerNewDF=players_df.selectExpr('*',"split(player,'-')[0] as playerName","split(player,'-')[1] as CountryCode","(split(`50s/100s`,'/')[0]+split(`50s/100s`,'/')[1])  as TotaScore").filter(col('TotaScore')>90)

FinalDF=PlyerNewDF.join(countries_df,PlyerNewDF.CountryCode==countries_df.SRT,how='inner').select(['playerName','country','TotaScore','runs'])

FinalDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2.Handle Bad Records from Csv File
# MAGIC
# MAGIC option('mode','FAILFAST') -- Exeuction Failed
# MAGIC
# MAGIC option('mode','DROPMALFORMED') -- Drop the Correcpt Record
# MAGIC
# MAGIC option('mode','corrupt_record') -- Capture the record
# MAGIC
# MAGIC df=spark.read.option('mode','corrupt_record').schema(schema).option("header",True).
# MAGIC option("columnNameofCorreuptRecord","corrupt_record").csv("Path")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3. Create Widgets and pass parameter from Datafactory
# MAGIC
# MAGIC widget --> dbutils.widgets.text('city')
# MAGIC
# MAGIC get value to varaible --> city=dbutils.widgets.get('city')
# MAGIC
# MAGIC Passs city value parameter from Datafactory to Databricks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Q4 We need to Get Top3 pickup locations.
# MAGIC
# MAGIC ### Lets see how we can achieve this by using GroupBy count and limit.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
    StructField("reqid", IntegerType(), True),
    StructField("pickup_location", StringType(), True)
])
data = [(48, "Airport"), (49, "Office"),(50, "Hospital"),(51, "Airport"),(52, "Hospital"),(53, "Shoppingmall"),(54, "Office"),(55, "Hospital"),(56, "Hospital")]
pickup_df = spark.createDataFrame(data, schema)
pickup_df.display()


# COMMAND ----------

pickup_df.createOrReplaceTempView("PickupTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC select pickup_location,count(reqid) as cnt from PickupTable group by pickup_location order by cnt desc limit 3
# MAGIC #or
# MAGIC select pickup_location from (select pickup_location,row_number() over(order by cnt desc) as rownum from (select pickup_location,count(reqid) as cnt from PickupTable group by pickup_location order by cnt desc)) where rownum<=3

# COMMAND ----------

##using Pyspark row Number

from pyspark.sql.functions import desc,row_number,col,count
from pyspark.sql.window import Window

Windowspec = Window.orderBy(col('cnt').desc())
pickDf = pickup_df.groupBy(col("pickup_location")).agg(count("reqid").alias("cnt"))
pickDfrow = pickDf.withColumn('Rownum',row_number().over(Windowspec)).filter(col('Rownum')<=3)
pickDfrow.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Q5 We need to Get non repeated employee details.

# COMMAND ----------

data = [
    (100, 'IT', 100, '2024-05-12'),
    (200, 'IT', 100, '2024-06-12'),
    (100, 'FIN', 400, '2024-07-12'),
    (300, 'FIN', 500, '2024-07-12'),
    (300, 'FIN', 1543, '2024-07-12'),
    (300, 'FIN', 1500, '2024-07-12')
]
columns = ["empid", "dept", "salary", "date"]
df = spark.createDataFrame(data, columns)

# COMMAND ----------

WindowspecPar = Window.partitionBy(col("empid"))
df.withColumn('cnt',count(col("empid")).over(WindowspecPar)).filter(col('cnt')==1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q6 Get only Integer Rows 

# COMMAND ----------

# MAGIC %sql
# MAGIC df = spark.table("emp_new")CREATE TABLE emp_new (employee_id VARCHAR(50));
# MAGIC INSERT INTO emp_new (employee_id) VALUES ('72657'),('1234'),('Tom'),('8792'),('Sam'),('19998'),('Philip');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_new where employee_id rlike('[0-9]')

# COMMAND ----------

df = spark.table("emp_new")
# or
#df = spark.read.table("emp_new")
# or
#df=spark.sql("select * from emp_new")

# COMMAND ----------

from pyspark.sql.functions import cast
from pyspark.sql.types import IntegerType
df.withColumn("Castcol",col("employee_id").cast(IntegerType())).filter(col('CastCol').isNotNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q7 Make First Letter Capital and Get rows which are not present in df2

# COMMAND ----------

data = [("virat kohli",), ("p v sindhu",)]
columns = ["name"]
df = spark.createDataFrame(data, columns)
data1 = [(1, 'Bob'), (2, 'Alice'), (3, 'Tom')]
data2 = [(1, 'Bob'), (3, 'Tom')]
df1 = spark.createDataFrame(data1, ["id", "name"])
df2 = spark.createDataFrame(data2, ["id", "name"])

# COMMAND ----------

#Make initial letter Capital
from pyspark.sql.functions import initcap,col
df.withColumn('name1',initcap(col('name'))).show()

# COMMAND ----------

#Records from df1 which are not available df2
df1.join(df2,on='id',how='leftanti').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q8 Count only null values from each column.

# COMMAND ----------

data = [(1, None, 'ab'),
    (2, 10, None),
    (None, None, 'cd')]
columns = ['col1', 'col2', 'col3']
df = spark.createDataFrame(data, columns)


# COMMAND ----------

from pyspark.sql.functions import col,sum
df.select([sum(col(colum).isNull().cast('int')).alias(colum) for colum in df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q9 Adding Prefix to all the Columns.

# COMMAND ----------

#Method1
from pyspark.sql.functions import concat,lit
prefix='Pre_'
df_Prefixed = df.select([col(c).alias(prefix+c) for c in df.columns])
df_Prefixed.display()
      

# COMMAND ----------

#Method2
for c in df.columns:
    prefix='Pre_'
    df = df.withColumnRenamed(c,prefix+c)
df.display()

# COMMAND ----------


