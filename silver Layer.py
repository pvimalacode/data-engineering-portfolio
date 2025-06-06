# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery 
from delta.tables import DeltaTable

# COMMAND ----------

class build_silver():
    def __init__(self,duplicates: list[str], bronze_table, schema_name, silver_table_checkpoint,silver_table ):
        self.duplicates = duplicates
        self.bronze_table = bronze_table
        self.schema = schema_name
        self.silver_table_checkpoint = silver_table_checkpoint
        self.silver_table = silver_table
        

    def get_watermark(self):
        bronze_version = DeltaTable.forName(spark, self.schema+'.'+self.bronze_table).history()
        start_time = str(bronze_version.filter(bronze_version.operation == 'STREAMING UPDATE').agg({'timestamp':'max'}).collect()[0][0])
        
        return str(start_time)
        
       
    
    def load_bronze_cdc(self)-> DataFrame:
        start_time = self.get_watermark()
        df = spark.readStream \
                    .option("readChangeFeed", "true") \
                    .option("startingTimestamp", start_time) \
                    .table(f"{self.schema}.{self.bronze_table}")\
                    .dropDuplicates(self.duplicates)
        return df

    def upsert_to_silver(self, batch_df:DataFrame, batch_id:int):
    
        if batch_df.isEmpty():
            return
        batch_df.createOrReplaceTempView("df")
        silver = self.schema +'.'+self.silver_table
        query = f"""MERGE INTO {silver} AS t
                            USING df as s
                            ON t.id = s.id

                        WHEN MATCHED THEN
                        UPDATE SET  t.id = s.id ,
                                    t.firstName = s.firstName ,
                                    t.middleName = s.middleName ,
                                    t.lastName= s.lastName ,
                                    t.gender = s.gender,
                                    t.birthDate = s.birthDate,
                                    t.ssn = s.ssn,
                                    t.salary = s.salary ,
                                    t.bronze_version = s._commit_version,
                                    t.change_type = s._change_type,
                                    t.commit_timestamp = s._commit_timestamp

                    WHEN NOT MATCHED THEN INSERT (id,firstName,middlename,lastName,gender,birthDate,ssn,salary,bronze_version,change_type,commit_timestamp)
                    VALUES (s.id, s.firstName, s.middleName,s.lastName,s.gender,s.birthDate,s.ssn,s.salary,s._commit_version,s._change_type,s._commit_timestamp)
                    """
        batch_df.sparkSession.sql(query)  
    
    def load_silver(self,bronze_cdc : DataFrame) -> StreamingQuery:
        return bronze_cdc.writeStream\
                    .foreachBatch(self.upsert_to_silver)\
                    .option("checkpointLocation",self.silver_table_checkpoint)\
                    .trigger(availableNow = True)\
                    .start()
        
    def start_silver_load(self):
        df = self.load_bronze_cdc()
        query = self.load_silver(df)
        query.awaitTermination()





# COMMAND ----------

dup = ['id']
bronze_table = 'customer_v1'
schema_name ='default' 
silver_table_checkpoint = '/dbfs/user/clientdata/autoloader/customer_silver_v1/checkpoint_location'
silver_table = 'customer_silver_v1'

customer_silver = build_silver(dup,bronze_table, schema_name, silver_table_checkpoint,silver_table)



# COMMAND ----------

customer_silver.start_silver_load()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from default.customer_silver_v1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.customer_silver_v1 limit 10;