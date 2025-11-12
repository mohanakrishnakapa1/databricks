# Databricks notebook source
# MAGIC %md
# MAGIC ### Video on:
# MAGIC          1.All Pupose Compute
# MAGIC          2.Access Modes
# MAGIC          3.Cluster Permissions
# MAGIC          4.Policies
# MAGIC          5.Job Compute

# COMMAND ----------

# MAGIC %md
# MAGIC ### All Purpose Compute:
# MAGIC             Clutster:
# MAGIC             
# MAGIC                           1.Name
# MAGIC
# MAGIC                           2.Policy --> Restric (setup Policies)
# MAGIC
# MAGIC                           3.Single or Mutinode
# MAGIC
# MAGIC                           4.Acces Mode --> Single user or Shared or No Isolation shared--(This one used for legacy Hive metastore. IF the Legacy client still want it)
# MAGIC
# MAGIC                           5.Databricks Runtime Versions (Standard or ML) --> Scala or Spark Versions (LTS--Long Term Support)
# MAGIC
# MAGIC                           6.Use Photon --> (Jobs can Execute much faster. (but it increases the cost))
# MAGIC
# MAGIC                           7.Workers Type --> Type of VM (Select as per our Memory and Cores Requirements)  --> can define Min Worker and Max Workers (Driver Option)
# MAGIC                           
# MAGIC                           8.Enabling autoscaling
# MAGIC
# MAGIC                           9.Terminate aFter (100 Min..etc)
# MAGIC
# MAGIC                           10. Tags
# MAGIC
# MAGIC                           11.Spark Config
# MAGIC
# MAGIC                           12.Logging
# MAGIC
# MAGIC                           13. Init Scripts
# MAGIC
# MAGIC                           14. Libraries
# MAGIC
# MAGIC                           15.Event log 
# MAGIC
# MAGIC                           16. Spark UI --> Debug Jobs
# MAGIC
# MAGIC                           17.Driver logs
# MAGIC
# MAGIC                           18. Metrics
# MAGIC
# MAGIC                           19. Apps
# MAGIC
# MAGIC                 Edit Permissions:
# MAGIC                          
# MAGIC                         1. Can Manage
# MAGIC
# MAGIC                         2. Can Restart
# MAGIC
# MAGIC                         3. Can Attache To
# MAGIC
# MAGIC
# MAGIC               Policies: The Worker nodes and other options will changes based on selected Policiy.
# MAGIC
# MAGIC                         We can create Custom policies as well.
# MAGIC                         
# MAGIC
# MAGIC                          1.UnRestricted Compute
# MAGIC
# MAGIC                          2.Shared Compute
# MAGIC
# MAGIC                          3.Power user Compute
# MAGIC
# MAGIC                          4.Legacy Shared Compute
# MAGIC
# MAGIC                          5. Personal Compute 
# MAGIC                          
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Job Compute : only to run the Jobs, Once the job completes then it will automatically kills

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.Server less Compute

# COMMAND ----------

# MAGIC %md
# MAGIC ### **************Pools: These are useful when we want to start the Compute Immediately.
# MAGIC
# MAGIC When we create and attach the pools then We don't need to wait for Compute to start as compute will use pools then compute will start automatically

# COMMAND ----------


