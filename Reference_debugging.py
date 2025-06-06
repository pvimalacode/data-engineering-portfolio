# Databricks notebook source
# MAGIC %run /Users/mail.pvimala@gmail.com/Databricks_Basics/ETL_customer/customer_raw_data_set_u

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog()

# COMMAND ----------

location = '/dbfs/user/clientdata/customer'

# COMMAND ----------

x = dbutils.fs.ls(location)

for f in x:
    if f.name.endswith(".parquet"):
        print(f.path)

# COMMAND ----------

def checkIfDelta(location):
    for i in dbutils.fs.ls(location):
        if f.name == '_delta_log':
            return 'it\'s a delta file'
    return 'The file is not delta'
location = 'dbfs:/databricks/driver/customer'
checkIfDelta(location)


# COMMAND ----------

df.createOrReplaceTempView("customer")
df2 = spark.sql("select id,count(*) as counts  from customer group by id having count(*)>1 ")
df2.show()

# COMMAND ----------

spark.read.parquet("dbfs/dbfs/user/clientdata/customer/part-00000-tid-785169333921980031-8cce03d6-6c59-4acd-8efd-83fc7a44ccff-2584-1-c000.snappy.parquet")

# COMMAND ----------

dbutils.fs.rm("/dbfs/databricks/delta",True) 

# COMMAND ----------

path = '/dbfs/dbfs/user/clientdata'
#dbutils.fs.rm(path)
dbutils.fs.rm(path,True)
#/dbfs/user/clientdata/autoloader/customer

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = true
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum delta.`/dbfs/databricks/delta/customer_details` retain 0 hours dry run;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum delta.`dbfs:/dbfs/databricks/delta/customer` retain 0 hours ;

# COMMAND ----------

dbutils.fs.rm("dbfs:/dbfs/databricks/delta/customer",True)

# COMMAND ----------

# MAGIC %md
# MAGIC **To delete a table** <br>
# MAGIC `drop table table_name;` <br> 
# MAGIC - Need to delete the schema to apply new schema <br>
# MAGIC - for that retention period must be overwritten <br> <br>
# MAGIC `set spark.databricks.delta.retentionDurationCheck.enabled = true`
# MAGIC <br><br>
# MAGIC - Then need to delete the delta table location where it is stored <br>
# MAGIC `vacuum delta.`dbfs:/dbfs/databricks/delta/customer` retain 0 hours dry run;` <br> <br>
# MAGIC - After dry run run the vaccum command
# MAGIC <br>
# MAGIC vacuum delta.`dbfs:/dbfs/databricks/delta/customer` retain 0 hours; <br><br>
# MAGIC dbutils.fs.rm("dbfs:/dbfs/databricks/delta/customer",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(bronze_version) from default.customer_silver_v1

# COMMAND ----------


start_version_df = spark.sql("""
          select max(bronze_version) from default.customer_silver_v1
          """)

start_version = start_version_df.collect()[0][0]
if  start_version:
    #print("i am here")
    pass
else:
    start_version = 1
    
# if start_version_df.collect()[0][0].isNull():
#     start_version = 0
#     print(0)
# else:
#     print(type(start_version_df.collect()[0][0]))
#     print('not working')
print(start_version)
#start_version_df.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY default.customer_v1

# COMMAND ----------

spark.read\
        .option("readChangeFeed", "true") \
        .option("startingVersion", start_version) \
        .option("endingVersion",end_version) \
        .table("default.customer_v1")

# COMMAND ----------

customer_raw= customer_raw.dropDuplicates(["id"])
customer_raw.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO default.customer_silver_v1 AS t
# MAGIC USING source_table as s
# MAGIC ON t.id = s.id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET  t.id = s.id ,
# MAGIC             t.firstName = s.firstName ,
# MAGIC             t.middleName = s.middleName ,
# MAGIC             t.lastName= s.lastName ,
# MAGIC             t.gender = s.gender,
# MAGIC             t.birthDate = s.birthDate,
# MAGIC             t.ssn = s.ssn,
# MAGIC             t.salary = s.salary ,
# MAGIC             t.ingestion_time = s.ingestion_time,
# MAGIC             t.bronze_version = '{end_version}'
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (id,firstName,middlename,lastName,gender,birthDate,ssn,salary,ingestion_time,bronze_version)
# MAGIC VALUES (s.id, s.firstName, s.middleName,s.lastName,s.gender,s.birthDate,s.ssn,s.salary,s.ingestion_time,'{end_version}')
# MAGIC         
# MAGIC             -- WhEN MATCHED and source.isDelete = 'Y' then
# MAGIC             -- UPDATE SET target.isDelete = 'Y', 
# MAGIC             --             target.end_date = current_timestamp(),
# MAGIC             --             target.updated_at = current_timestamp()
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('default.customer_v1',2 )

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from customer_v1

# COMMAND ----------


bronze_version = spark.sql("""
                    DESCRIBE HISTORY default.customer_v1 
                        """)
bronze_version =   bronze_version.filter(bronze_version.operation == 'STREAMING UPDATE')                     
end = bronze_version.agg({'version':'max'}).collect()[0][0]
start = bronze_version.agg({'version':'min'}).collect()[0][0]
 
start,end


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY default.customer_v1 