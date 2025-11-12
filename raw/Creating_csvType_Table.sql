-- Databricks notebook source


-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /FileStore/tables/
-- MAGIC

-- COMMAND ----------

show databases;

-- COMMAND ----------

create database f1_raw;

-- COMMAND ----------

show databases;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###########Circuite Table

-- COMMAND ----------

use f1_raw;

-- COMMAND ----------

create table f1_raw.circuits(circuitID INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng STRING,
alt INT,
url STRING
)
USING csv
Options(path "/FileStore/tables/circuits-1.csv",header true)

-- COMMAND ----------

select * from circuits;

-- COMMAND ----------

create table f1_raw.races(raceID INT,
year INT,
round INT,
circuitID INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING csv
Options(path "/FileStore/tables/races.csv",header true)

-- COMMAND ----------

select * from races

-- COMMAND ----------


