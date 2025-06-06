# Databricks notebook source
from pyspark.sql.functions import input_file_name, current_timestamp
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery 

# COMMAND ----------

class build_bronze():
    def __init__(self, schema ,raw_data, table_name, stream_location ):
        self.raw_data = raw_data
        self.table_name = table_name
        self.location_path = stream_location
        self.schema = schema

    def read_rawdata(self) -> DataFrame:
        df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","parquet")
        .option("cloudFiles.schemaLocation",f"{self.location_path}{self.table_name}/schema_location")
        .option("pathGlobFilter","*.parquet")
        .option("recursiveFileLookup","true")
        .load(self.raw_data)
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("source_file", input_file_name())
        )
        return df

    def write_bronze_table(self,df) -> StreamingQuery:
        return (df.writeStream
        .option("checkpointLocation",f'{location_path}{table_name}/checkpoint_location')
        .option("path",f'{location_path}{table_name}/output')
        .option("badRecordsPath", f"{location_path}badRecordsPath")
        .option("mergeSchema", "true")
        .trigger(availableNow = True)
        .table(self.schema+self.table_name))

    def start_bronze_load(self):
        df = self.read_rawdata()
        query = self.write_bronze_table(df)
        query.awaitTermination()



# COMMAND ----------

location_path = '/dbfs/user/clientdata/autoloader/'
raw_data ='/dbfs/user/clientdata/customer' 
schema = 'default.'
table_name = 'customer_v1'


customer_bronze = build_bronze(schema ,raw_data, table_name, location_path)


# COMMAND ----------

customer_bronze.start_bronze_load()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from default.customer_v1

# COMMAND ----------

