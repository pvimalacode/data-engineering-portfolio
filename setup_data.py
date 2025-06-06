# Databricks notebook source
#setting up input files
#get the sample data from below and save to a location
#load duplicates into load1.csv
#https://www.kaggle.com/datasets/asthamular/people-10-m

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType


# COMMAND ----------


schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("firstName", StringType(), True),
  StructField("middleName", StringType(), True),
  StructField("lastName", StringType(), True),
  StructField("gender", StringType(), True),
  StructField("birthDate", TimestampType(), True),
  StructField("ssn", StringType(), True),
  StructField("salary", IntegerType(), True)
])


# COMMAND ----------

def load_data(input_path,output_path,parts):
    df = spark.read.format("csv").option("header", True).schema(schema).load(input_path)
    df.repartition(parts) \
        .write \
        .mode("overwrite") \
        .parquet(output_path)
    print(type(df))
    return display(dbutils.fs.ls(output_path))



# COMMAND ----------

input_path = "dbfs:/FileStore/dbfs/Volumes/main/default/my-volume/export.csv"
output_path = "/dbfs/user/clientdata/customer/2025/01"
parts = 10
load_data(input_path,output_path,parts)


# COMMAND ----------

input_path2 ='dbfs:/FileStore/load1.csv'
out_path2 = '/dbfs/user/clientdata/customer/2025/03'
parts = 1
load_data(input_path2,out_path2,parts)


# df.coalesce(1) \
#   .write \
#   .mode("overwrite") \
#   .parquet('/dbfs/user/clientdata/customer/2025/03')



# COMMAND ----------

# data = [(421,'Davina','Idella','Decker','M','1986-03-30T05:00:00.000+00:00','905-52-9038',66244),
#         (573,'Jacquie','Leonore','McCrum','F','1986-03-30T05:00:00.000+00:00','911-74-9546',71327)]

# columns = ['id',
#  'firstName',
#  'middleName',
#  'lastName',
#  'gender',
#  'birthDate',
#  'ssn',
#  'salary']

# output_path = "/dbfs/user/clientdata/customer/2025/02"
# df1 = spark.createDataFrame(data, columns)
# df1.coalesce(1).write.mode("overwrite").parquet(output_path)
# display(dbutils.fs.ls(output_path))
# #df1.display()

# COMMAND ----------

