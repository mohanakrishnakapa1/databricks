# Databricks notebook source
# MAGIC %md
# MAGIC ### OverView of Azure Databricks Console
# MAGIC
# MAGIC 1.Workspace --> Workarea
# MAGIC
# MAGIC 2.Catalog --> to create Metastore
# MAGIC
# MAGIC 3.User Management --> 
# MAGIC
# MAGIC       a. Users = Individual users with Roles like Account admin..etc
# MAGIC
# MAGIC       b. Service Prinicipals = Service Accounts/Common Account users.
# MAGIC       
# MAGIC       c. Groups = To Manage users
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### How Databrticks with Azure? How the Information stored in Backend of Azure?
# MAGIC
# MAGIC 1. We create a Resource group with in Azure while setup the Databricks account in azure.
# MAGIC
# MAGIC 2. Along with resource group a Vnet and Network security group will be created.
# MAGIC
# MAGIC 3. Also a Managed resource group will be created. This will contains information related to Databricks storage account.If we mention any external storage then Databricks will use external storage account.
# MAGIC
# MAGIC 4. Once we start the cluster then a VM's were created at the Managed Resource groups related to Databricks.Here all the clusters and related VM's were created and manage at Managed Resource group.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog:(Open Source)
# MAGIC
# MAGIC #### Centralized
# MAGIC
# MAGIC It Offers:
# MAGIC
# MAGIC       a.Security
# MAGIC
# MAGIC       b.Auditing
# MAGIC
# MAGIC       c.Lineage
# MAGIC
# MAGIC       d.Data Discovery
# MAGIC
# MAGIC Component:
# MAGIC    1. MetaStore  --> Catalog  --> Schema 
# MAGIC
# MAGIC                                     --> Tables
# MAGIC
# MAGIC                                     --> View
# MAGIC
# MAGIC                                     --> Volume
# MAGIC
# MAGIC                                     --> Model
# MAGIC
# MAGIC                                     --> Function
# MAGIC
# MAGIC
# MAGIC    Catalog--> To Secure data.
# MAGIC
# MAGIC       Table,Views are used to maintain Structured Data.
# MAGIC
# MAGIC       Volume is File system to main Structured,Semi and Unstructured Data.
# MAGIC
# MAGIC       Model --> YAML models.
# MAGIC
# MAGIC   Access Objects from 3 level Name Spaces.
# MAGIC       
# MAGIC       Catalog.schema.<tableName>
# MAGIC
# MAGIC          Ex: dw.silver.sales
# MAGIC
# MAGIC
# MAGIC
# MAGIC          
# MAGIC       
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Metastore
# MAGIC
# MAGIC Create a Metastore:
# MAGIC
# MAGIC Name: <> its better to have a single metastore per region.
# MAGIC
# MAGIC Steps:
# MAGIC
# MAGIC         1.Name
# MAGIC
# MAGIC         2.Region
# MAGIC
# MAGIC         3.Storage account path <ADLS Path> Ex:root@adfseasewithdata01.dfs.windows.net/metastore
# MAGIC
# MAGIC         4. Access Connector for Azure to ADLS Storage.
# MAGIC
# MAGIC         5. Access Connector ID (Mention the Connector ID)
# MAGIC
# MAGIC         6. Assign the Workspace
# MAGIC
# MAGIC
# MAGIC
# MAGIC         
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Manage and External Catalog
# MAGIC
# MAGIC **** If mention the External at catalog or Schema level thne data storage location will changes.
# MAGIC
# MAGIC **** If not mentioned any location then data will be stored in default METASTORE location.
# MAGIC
# MAGIC Add Catalog --> Type (Standard, Foreign() and Shared) --> Storage location (without External location )
# MAGIC
# MAGIC Command:
# MAGIC
# MAGIC DESCRIBE CATALOG EXTENDED Dev;  --> since we created catalog using UI and we have not mentioned any location so once execute this query we don't see any storage location.
# MAGIC
# MAGIC Create catalog using SQL:
# MAGIC
# MAGIC CREATE CATALOG dev_sql COMMENT 'THis catalog created using SQL'
# MAGIC
# MAGIC DESCRIBE CATALOG EXTENDED dev_sql;
# MAGIC
# MAGIC
# MAGIC DROP CATALOG:
# MAGIC
# MAGIC DROP CATALOG dev_sql; --> ERROR: 'dev_sql' is not empty.As the catalog by default create some schemas we have to delete these schemas before delete the catalog, or else we can use below command to delete forceful/Recusively.
# MAGIC
# MAGIC DROP CATALOG dev_sql CASCADE;
# MAGIC
# MAGIC
# MAGIC Create Catalog with External location:
# MAGIC
# MAGIC To create this we need to first create a new external location or use existing one.
# MAGIC
# MAGIC New --> create External location --> Name --> Storage crendetial(Bridge between databricks and storage location) --> URL 
# MAGIC
# MAGIC Use SQL to define external location:
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION 'ext_catalog'
# MAGIC URL 'abfss://data@adbeasewithdata01.dfs.core.windows.net/adb/catalog'
# MAGIC with (STIRAGE CREDENTIAL 'sc_Catalog_storage')
# MAGIC
# MAGIC Create catalog with external location:
# MAGIC
# MAGIC CREATE CATALOG dev_ext MANAGE LOCATION 'abfss://data@adbeasewithdata01.dfs.core.windows.net/adb/catalog'
# MAGIC COMMENT 'This is external location catlog'
# MAGIC
# MAGIC Check: DESCRIBE CATALOG EXTENDED dev_ext;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema with and Without External locations
# MAGIC
# MAGIC without external schema/catalog location:
# MAGIC
# MAGIC CREATE SCHEMA dev.bronze
# MAGIC COMMENT 'with out external location';
# MAGIC
# MAGIC
# MAGIC with external catalog location:
# MAGIC
# MAGIC CREATE SCHEMA dev_ext_catalog.bronze
# MAGIC COMMENT 'with catalog external location';
# MAGIC
# MAGIC
# MAGIC [ CREATE AN External location for Schema:(This is same as above we defined in catalog, only change is path and Name of the location)
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION 'ext_schema' URL 'abfss://data@adbeasewithdata01.dfs.core.windows.net/adb/schema/bronze_ext' with (STIRAGE CREDENTIAL 'sc_Catalog_storage') ]  
# MAGIC
# MAGIC Create schema with external location:
# MAGIC
# MAGIC CREATE SCHEMA dev_ext.bronze_ext
# MAGIC LOCATION 'abfss://data@adbeasewithdata01.dfs.core.windows.net/adb/schema'
# MAGIC COMMENT 'Schema with external location';
# MAGIC
# MAGIC Now If we create and store the location then it is based on the Catalog/Schema external location.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table with(Managed Table) and without External(External table) locations
# MAGIC
# MAGIC Same as schema. 
# MAGIC
# MAGIC But only difference is Managed table is stored in Metastore if we don't mention any external location.
# MAGIC
# MAGIC *** Another benifit using unity catlog over hive Metastore we can restore the Table with data with in 7 Days.***
# MAGIC
# MAGIC External table with location, we can delete the table but data will be always present.
# MAGIC
# MAGIC To show the undrop tables from catalog.
# MAGIC
# MAGIC use catalog Dev;
# MAGIC
# MAGIC show tables dropped in bronze;
# MAGIC
# MAGIC Undrop table:
# MAGIC
# MAGIC updrop table dev.bronze.sales_manged_Tab;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Views,CASTS,Deep Clone and shallow clone
# MAGIC View: 
# MAGIC
# MAGIC             Temporary View:
# MAGIC
# MAGIC             Terminate view after session teminate.
# MAGIC
# MAGIC             CREATE TEMPORARY VIEW emp_tmp_vw
# MAGIC             AS
# MAGIC             select * from dev.bronze.emp
# MAGIC             where dept_code='D101'
# MAGIC
# MAGIC             Permenant View:
# MAGIC
# MAGIC             CREATE OR REPLACE VIEW dev.bronze.emp_tmp_vw
# MAGIC             AS
# MAGIC             select * from dev.bronze.emp
# MAGIC             where dept_code='D102'
# MAGIC
# MAGIC
# MAGIC To create copy of data (CTAS --> Create table as it is) :
# MAGIC
# MAGIC             Create TABLE dev.bronze.emp_ctas as select * from dev.bronze.emp
# MAGIC
# MAGIC             describe extended dev.bronze.emp_ctas --> This will be in different location as we are duplicating the data.
# MAGIC
# MAGIC             describe history dev.bronze.emp_ctas  --> As this starts with version 0.
# MAGIC
# MAGIC
# MAGIC Deep Clone :(There is risk of losing meta data and partition so better to use deep close)
# MAGIC
# MAGIC             Copy the metadata * data --> extact replica of Source table
# MAGIC
# MAGIC             CREATE TABLE dev.bronze.emp_dc DEEP CLOSE dev.bronze.emp;
# MAGIC
# MAGIC             describe extended dev.bronze.emp_dc;
# MAGIC
# MAGIC             describe history dev.bronze.emp_dc;
# MAGIC
# MAGIC
# MAGIC Shallow Colne:
# MAGIC
# MAGIC             Only Metadata is clone. This will point to the Current version of the data and show the data from source table.
# MAGIC
# MAGIC             If insert or perform any operations it will not show in shallow clone table.
# MAGIC
# MAGIC             Support we have V3 when we create Shallow table the if perform any now the current veriosn of the source table is V4,
# MAGIC
# MAGIC             but when you run shallow table select it will still show the V3 data.
# MAGIC
# MAGIC             It will be same if we insert a record it will not effect the Source table.
# MAGIC
# MAGIC             Only this table version will get updated when when perform VaCCUM.
# MAGIC
# MAGIC             CREATE TABLE dev.bronze.emp_sc SHALLOW CLOSE dev.bronze.emp;
# MAGIC
# MAGIC             describe extended dev.bronze.emp_sc;
# MAGIC
# MAGIC             describe history dev.bronze.emp_sc;  
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Important Queries
# MAGIC
# MAGIC #### list catalogs
# MAGIC
# MAGIC show catalogs;
# MAGIC
# MAGIC show catalogs like 'dev*'
# MAGIC
# MAGIC show catalogs like '*m*'
# MAGIC
# MAGIC
# MAGIC #### List Schemas & Tables
# MAGIC
# MAGIC Show schemas in dev
# MAGIC
# MAGIC Show schemas in dev like 'in*'
# MAGIC
# MAGIC show tables in dev.broze
# MAGIC
# MAGIC show tables in dev.broze like 'sale*'
# MAGIC
# MAGIC #### Check if the table exists in schema
# MAGIC
# MAGIC spark.catalog.tableExists("dev.bronze.sales_external")
# MAGIC
# MAGIC spark.catalog.tableExists("dev.bronze.sales_external_1")
# MAGIC
# MAGIC #### Check History
# MAGIC
# MAGIC describe history dev.bronze.emp;
# MAGIC
# MAGIC describe extended dev.bronze.emp;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deletion verctors & Liquid clustering
# MAGIC
# MAGIC
# MAGIC #### Deletion Vector:
# MAGIC
# MAGIC         Added flag/Marked to the deleted record and it will not show the final results when run the select query.
# MAGIC
# MAGIC         So this will reduce the entire parquet file rewriiten. 
# MAGIC
# MAGIC         Once we run maintainance OPTIMIZED or Predective maintainance automatically parquest file rewritten and deleted all the records.
# MAGIC #### Liquid Clustering:
# MAGIC
# MAGIC                 Improves the existing partitioning and Zorder technique.
# MAGIC
# MAGIC                 provides flexibility to redefine clustering columns ** without rewriting exiting data **
# MAGIC         
# MAGIC         Liquid clustering used for:
# MAGIC
# MAGIC                 Table often fliterd by High cardinality columns
# MAGIC
# MAGIC                 Skeness with data
# MAGIC
# MAGIC                 Data grow quickly
# MAGIC
# MAGIC                 typically column could leave the table with too many or too few partitions
# MAGIC
# MAGIC                 ALTER TABLE dev.bronze.sales CLUSTER BY (INVOICENO);
# MAGIC
# MAGIC                 describe history dev.bronze.sales;
# MAGIC
# MAGIC                 CREATE TABLE dev.bronze.sales_ct CLUSTER BY (INVOICENO) --> add cluster while creating the table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## VOLUME:
# MAGIC       Can store the files data directly and access using volumes.
# MAGIC           Data can be:
# MAGIC
# MAGIC               1.Strctured
# MAGIC
# MAGIC               2.Semi strctured
# MAGIC
# MAGIC               3.Unstrctured
# MAGIC       
# MAGIC       Volumnes are two types 1. Managed 2. External
# MAGIC
# MAGIC       Managed Vcolume:
# MAGIC
# MAGIC              CREATE VOLUME dev.bronze.managed_vlm
# MAGIC
# MAGIC              COMMENT "This is Mange Volume"
# MAGIC
# MAGIC           access using selected after csv file copied to Volume:
# MAGIC
# MAGIC              select * from csv."volume/dev/bronze/managed_vlm/files/emp.csv"
# MAGIC      
# MAGIC      External Vcolume:
# MAGIC
# MAGIC              CREATE EXTERNAL VOLUME dev.bronze.external_vlm
# MAGIC
# MAGIC              COMMENT "This is Extarnal Volume"
# MAGIC
# MAGIC              LOCATION "abfss://data@adbeasewithdata01.dfs.core.windows.net/adb/volumne/ext" 
# MAGIC
# MAGIC             select * from csv."volume/dev/bronze/external_vlm/files/emp.csv"
# MAGIC
# MAGIC     DROP VOLUMNE dev.bronze.manage_vol

# COMMAND ----------


