-- Databricks notebook source
-- MAGIC %md
-- MAGIC Create constructors table
-- MAGIC  *Single Line JSON
-- MAGIC  *Simple Structure

-- COMMAND ----------

create table f1_raw.constructor(constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
Options(path "/FileStore/tables/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructor

-- COMMAND ----------

drop table if exists f1_raw.driver;
create table f1_raw.driver(driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename:STRING, surname:STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
Options(path "/FileStore/tables/drivers.json")

-- COMMAND ----------

select * from f1_raw.driver

-- COMMAND ----------

create table f1_raw.results(
  resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed STRING,
statusId INT
)
using json
options (path "/FileStore/tables/results.json")

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

drop table f1_raw.pits;
create table f1_raw.pits(
raceId INT,
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
stop INT,
time STRING
)
using json
options (path "/FileStore/tables/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_raw.pits

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.json("/FileStore/tables/pit_stops.json"))

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /FileStore/tables/

-- COMMAND ----------


