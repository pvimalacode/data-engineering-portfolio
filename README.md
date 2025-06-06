# data-Engineering-portfolio


This is a simple ETL project in databricks using delta tables
1. Cleans up environmnet to set up the tables and load files to consume
2. Loads raw data to a sink and creates bronze and silver table
3. craetes autoloder for bronze loading
4. creates silver table by implementing CDC and merges only new or deleted or updated records
