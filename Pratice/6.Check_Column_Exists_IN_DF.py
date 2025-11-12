# Databricks notebook source
cls=df.columns

# COMMAND ----------

fieldcheck=cls.count('id')
if fieldcheck > 0:
    print ("id column available")
else:
    print ("id column Not available")
