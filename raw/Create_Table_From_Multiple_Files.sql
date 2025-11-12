-- Databricks notebook source
create table f1_raw.lap_times(
raceId INT,
driverId INT,
duration STRING,
lap INT,
position INT,
time STRING,
milliseconds INT
)
using json
options (path "/FileStore/tables/lap_times_split_*.csv")

-- COMMAND ----------

select count(*) from f1_raw.lap_times;

-- COMMAND ----------

create table f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT
)
using json
options (path "/FileStore/tables/qualifying_split_*.json")

-- COMMAND ----------

select count(*) from f1_raw.qualifying

-- COMMAND ----------

DESCRIBE EXTENDED f1_raw.qualifying

-- COMMAND ----------

describe database f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Default Syntax to create table from parquet as external TAble

-- COMMAND ----------

df.write.mode("overwrite").formart("parquet").saveAsTable("<db.tablename>")
