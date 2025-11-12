# Databricks notebook source
cir_df= spark.read.parquet("/mnt/formula1/circuits")
display(cir_df)

# COMMAND ----------

rac_df=spark.read.parquet("/mnt/formula1/races")

# COMMAND ----------

rac_df.show(3)

# COMMAND ----------

join_df=cir_df.join(rac_df,cir_df.circuite_id==rac_df.circit_id,"inner")

# COMMAND ----------

join_df.show()

# COMMAND ----------

##Select only Required Columns
join_df=cir_df.join(rac_df,cir_df.circuite_id==rac_df.circit_id,"inner").select(cir_df.circuite_ref,cir_df.name,cir_df.location,cir_df.Country,rac_df.name.alias("race_name"),rac_df.round,rac_df.race_year,rac_df.ingestion_date)

# COMMAND ----------

join_df.show()

# COMMAND ----------

##same Outter Joins
join_df=cir_df.join(rac_df,cir_df.circuite_id==rac_df.circit_id,"left").select(cir_df.circuite_ref,cir_df.name,cir_df.location,cir_df.Country,rac_df.name.alias("race_name"),rac_df.round,rac_df.race_year,rac_df.ingestion_date)

# COMMAND ----------

##same Outter Joins
join_df=cir_df.join(rac_df,cir_df.circuite_id==rac_df.circit_id,"right").select(cir_df.circuite_ref,cir_df.name,cir_df.location,cir_df.Country,rac_df.name.alias("race_name"),rac_df.round,rac_df.race_year,rac_df.ingestion_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ########Semi Join You will get only columns from left table. It is similar to Inner Join. But only difference is you will get only columns from left table

# COMMAND ----------

##If you mention the columns it will throw the error
join_df=cir_df.join(rac_df,cir_df.circuite_id==rac_df.circit_id,"semi").select(cir_df.circuite_ref,cir_df.name,cir_df.location,cir_df.Country,rac_df.name.alias("race_name"),rac_df.round,rac_df.race_year,rac_df.ingestion_date)

# COMMAND ----------

join_df=cir_df.join(rac_df,cir_df.circuite_id==rac_df.circit_id,"semi")

# COMMAND ----------

# MAGIC %md
# MAGIC #######oposite to semi join. Every record(Not matched Records with right DF) on the left dataframe which is not found on the right table.

# COMMAND ----------

join_df=cir_df.join(rac_df,cir_df.circuite_id==rac_df.circit_id,"anti")

# COMMAND ----------


