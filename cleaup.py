# Databricks notebook source
#delete all the delta table _delta_log, Autoloader schema evolution and raw data

raw_data = '/dbfs/user/clientdata/customer'  #raw data
autoloder = '/dbfs/user/clientdata/autoloader/'      #for autoloader
delta = '/dbfs/databricks/delta'               #for delta table
silver = '/dbfs/databricks/delta/customer_silver_v1'
#silver_delta = '/dbfs/user/clientdata/autoloader/customer_silver_v1'

location = []
location.append(raw_data)
location.append(autoloder)
location.append(delta)
location.append(silver)

table_name = []
table_name += ['default.customer_silver_v1','default.customer_v1']
print(location,table_name, sep = '\n')


# COMMAND ----------

dbutils.fs.rm(raw_data,True)


# COMMAND ----------

dbutils.fs.rm(autoloder,True)


# COMMAND ----------

dbutils.fs.rm(delta,True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists customer_v1

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists customer_silver_v1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE or replace TABLE  default.customer_v1 (
# MAGIC id int,
# MAGIC firstName STRING,
# MAGIC middleName STRING,
# MAGIC lastName STRING,
# MAGIC gender STRING,
# MAGIC --gender VARCHAR(1)
# MAGIC birthDate TIMESTAMP,
# MAGIC ssn STRING,
# MAGIC salary int,
# MAGIC _rescued_data STRING,
# MAGIC ingestion_time TIMESTAMP,
# MAGIC source_file STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/dbfs/databricks/delta/customer_v1'
# MAGIC tblproperties (delta.enableChangeDataFeed= true)
# MAGIC
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE  default.customer_silver_v1 (
# MAGIC id int,
# MAGIC firstName STRING,
# MAGIC middleName STRING,
# MAGIC lastName STRING,
# MAGIC gender STRING,
# MAGIC --gender VARCHAR(1)
# MAGIC birthDate TIMESTAMP,
# MAGIC ssn STRING,
# MAGIC salary int,
# MAGIC bronze_version bigint,
# MAGIC change_type STRING,
# MAGIC commit_timestamp TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/dbfs/databricks/delta/customer_silver_v1';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.customer_v1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.customer_silver_v1;