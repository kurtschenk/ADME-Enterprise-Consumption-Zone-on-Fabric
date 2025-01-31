#!/usr/bin/env python
# coding: utf-8

# ## ADME_Setup
# 
# New notebook

# # Setup code for main data table, logging table and run info table
# 
# 
# Below is code to create the tables needed to export data from ADME/OSDU to Fabric. Use the variables "table_name", "logging_table" and "run_info_table" to give names to the tables. Run the setup code below first, the cell with the schemas, and then the cell that creates the table you want. Notice the setting "delete_existing_table". It is default set to "no" which means the code will do nothing if the table exists. If this is changed to yes existing table with the same name will be deleted and emptied.
# 
# ## Setup variables for ADME
# server - They URL to the ADME server. You can find this in the portal on the overview page of the ADME instance
# 
# Api URLs - These you do not have to change and can use the values already there.
# 
# data_partition - The name of the data partition you want to export from. Data partitions can be found in the Portal on the ADME instance. Find data partitions on the left side when you have opened the ADME instance.
# 
# legal_tag - This is the default value that will be put on the exported document if the original does not have a legal_tag
# 
# acl_viewer - This is the default value that will be put on the exported document if the original does not have a value
# 
# acl_owner - This is the default value that will be put on the exported document if the original does not have a value
# 
# authentication_mode - In this example we use "msal_interactive" See (insert link here) for more information
# 
# authority - "https://login.microsoftonline.com/xxxxx" where xxxx is your tenantid
# 
# scopes - ["xxxx/.default"], where xxxx is your client_id. NOTE this variable is a list and therefore it needs the square brackets even if it is only one value
# 
# client_id - this is the app id that was used to create the ADME instance
# 
# tenant_id - the tenant id. Search Tenant properties in Portal to find this value. This is the tenant where ADME resides.
# 
# redirect_uri - this value is set on the app used when creating the ADME instance. 
# 
# access_token_type - "keyvault", it is strongly recommended that you use a keyvault for the key to access AMDE
# 
# key_vault_name - the name of the key vault
# 
# secret_name - the name of the secret in the key vault
# 
# table_name - the name of the table you store data to in Fabric - put code at bottom of notebook
# 
# logging_table - the name of the table where logs will be stored
# 
# run_info_table - the name of the table where last run info is stored. This is used for delta loads since last run time
# 
# lakehouse_name - the name of the lakehouse where the delta tables will reside
# 
# ## How to use the variables
# 
# **The variables are called with config["variable"], for example: config["server"]**
# 

# In[ ]:


import pandas as pd
import json

# Correct the JSON string format and load it
config_json = '''
{
    "server": "",
    "crs_catalog_url": "/api/crs/catalog/v2/",
    "crs_converter_url": "/api/crs/converter/v2/",
    "entitlements_url": "/api/entitlements/v2/",
    "file_url": "/api/file/v2/",
    "legal_url": "/api/legal/v1/",
    "schema_url": "/api/schema-service/v1/",
    "search_url": "/api/search/v2/",
    "storage_url": "/api/storage/v2/",
    "unit_url": "/api/unit/v3/",
    "workflow_url": "/api/workflow/v1/",
    "data_partition_id": "",
    "legal_tag": "legal_tag",
    "acl_viewer": "acl_viewer",
    "acl_owner": "acl_owner",
    "authentication_mode": "msal_interactive",
    "authority": "",
    "scopes": [""],
    "client_id": "",
    "tenant_id": "",
    "redirect_uri": "",
    "access_token_type" : "keyvault",
    "key_vault_name" : "",
    "secret_name" : "",
    "table_name" : "main",
    "logging_table" : "logging_info",
    "run_info_table" : "run_info",
    "delete_existing_table" : "no"
}
'''

# Load the JSON string into a Python dictionary
config_dict = json.loads(config_json)

# Create a Series from the dictionary
config = pd.Series(config_dict)

#The number of documents in each batch. If you increase this you could see error messages about the load being too big
batch_size = 750

display(config)


# In[ ]:


from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, MapType

# Define the log schema
log_schema = StructType([
    StructField("log_id", StringType(), False),
    StructField("log_timestamp", TimestampType(), False),
    StructField("log_level", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("line_number", StringType(), False),
    StructField("message", StringType(), False)
])

# Define the data schema
schema = StructType([
    StructField("createTime", StringType(), True),
    StructField("kind", StringType(), True),
    StructField("authority", StringType(), True),
    StructField("namespace", StringType(), True),
    StructField("legal", StringType(), True),
    StructField("createUser", StringType(), True),
    StructField("source", StringType(), True),
    StructField("acl", StringType(), True),
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("version", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("data", StringType(), True),
    StructField("modifyUser", StringType(), True),
    StructField("modifyTime", StringType(), True),
    StructField("ancestry", StringType(), True),    
    StructField("ingestTime", StringType(), True)
])

run_info_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("run_timestamp", LongType(), False)
])


# In[ ]:


#Run this to create logging_info table
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime
import uuid

# Initialize Spark session
spark = SparkSession.builder \
    .appName("LoggingTableCreation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the target table name and path
table_name = config["logging_table"]
table_path = f"Tables/{table_name}"

# Check the config for whether to delete the existing table
delete_existing_table = config.get("delete_existing_table", "no").lower() == "yes"

# Check if the table exists
table_exists = DeltaTable.isDeltaTable(spark, table_path)

if table_exists and delete_existing_table:
    # If table exists and we need to delete/overwrite it
    deltaTable = DeltaTable.forPath(spark, table_path)
    deltaTable.delete()  # This deletes all records in the table, not the table itself
    
    # Create an empty DataFrame with the schema and overwrite the existing table
    empty_df = spark.createDataFrame([], log_schema)
    empty_df.write.format("delta").mode("overwrite").save(table_path)
    print(f"Existing table {table_name} at {table_path} was deleted and recreated with the schema.")

elif not table_exists:
    # If the table does not exist, create it
    empty_df = spark.createDataFrame([], log_schema)
    empty_df.write.format("delta").mode("overwrite").save(table_path)
    print(f"Table {table_name} created at {table_path} with the schema.")
else:
    # If the table exists and we should not delete it
    print(f"Table {table_name} already exists at {table_path}. No changes made.")



# In[ ]:


#Run this code to create the run_info table before batch export
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime
import uuid

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RunInfoTableCreation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the target table name and path
table_name = config["run_info_table"]
table_path = f"Tables/{table_name}"
print(table_path)

# Check the config for whether to delete the existing table
delete_existing_table = config.get("delete_existing_table", "no").lower() == "yes"

# Check if the table exists
table_exists = DeltaTable.isDeltaTable(spark, table_path)

if table_exists and delete_existing_table:
    # If table exists and we need to delete/overwrite it
    deltaTable = DeltaTable.forPath(spark, table_path)
    deltaTable.delete()  # This deletes all records in the table, not the table itself
    
    # Create an empty DataFrame with the schema and overwrite the existing table
    empty_df = spark.createDataFrame([], run_info_schema)
    empty_df.write.format("delta").mode("overwrite").save(table_path)
    print(f"Existing table {table_name} at {table_path} was deleted and recreated with the schema.")

elif not table_exists:
    # If the table does not exist, create it
    empty_df = spark.createDataFrame([], run_info_schema)
    empty_df.write.format("delta").mode("overwrite").save(table_path)
    print(f"Table {table_name} created at {table_path} with the schema.")
else:
    # If the table exists and we should not delete it
    print(f"Table {table_name} already exists at {table_path}. No changes made.")



# In[ ]:


#Run this code to create the main table before batch export
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime
import uuid

# Initialize Spark session
spark = SparkSession.builder \
    .appName("LoggingTableCreation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the target table name and path
table_name = config["table_name"]
table_path = f"Tables/{table_name}"

# Check the config for whether to delete the existing table
delete_existing_table = config.get("delete_existing_table", "no").lower() == "yes"

# Check if the table exists
table_exists = DeltaTable.isDeltaTable(spark, table_path)

if table_exists and delete_existing_table:
    # If table exists and we need to delete/overwrite it
    deltaTable = DeltaTable.forPath(spark, table_path)
    deltaTable.delete()  # This deletes all records in the table, not the table itself
    
    # Create an empty DataFrame with the schema and overwrite the existing table
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").mode("overwrite").save(table_path)
    print(f"Existing table {table_name} at {table_path} was deleted and recreated with the schema.")

elif not table_exists:
    # If the table does not exist, create it
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").mode("overwrite").save(table_path)
    print(f"Table {table_name} created at {table_path} with the schema.")
else:
    # If the table exists and we should not delete it
    print(f"Table {table_name} already exists at {table_path}. No changes made.")



# # Helper code
# Below are some cells with code to help developers check imported data and similar QA tasks

# In[ ]:


#Code to see imported data - copy this into a cell at the bottom of the batch export to check the number of documents in the lakehouse
# Insert the current timestamp
run_id = str(uuid.uuid4())
run_info_df = spark.createDataFrame([(run_id,)], ["run_id"])
run_info_df = run_info_df.withColumn("run_timestamp", current_timestamp())
run_info_df.write.insertInto("admelakehouse.run_info", overwrite=False)

# Display the 10 documents with the newest createTime
df_newest = spark.sql(f"SELECT * FROM admelakehouse.{table_name} ORDER BY createTime DESC LIMIT 10")
display(df_newest)

# Query to select all rows from the table
df_all = spark.sql(f"SELECT * FROM admelakehouse.{table_name}")

# Count the number of rows
num_documents = df_all.count()

# Print the number of documents
print(f"Number of documents in admelakehouse.{table_name}: {num_documents}")


# In[ ]:


#Testcode to see how many documents has been imported and checking for duplicates
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the target table name and path
table_name = config["table_name"]
target_table_path = f"Tables/{table_name}"

# Load the Delta table
bronze_table = DeltaTable.forPath(spark, target_table_path)

# Read the data into a DataFrame
df = bronze_table.toDF()

# Count the occurrences of each id
id_counts = df.groupBy("id").agg(count("id").alias("count"))

# Filter for duplicate ids (count > 1)
duplicate_ids = id_counts.filter(col("count") > 1)

# Show duplicate ids if any
if duplicate_ids.count() > 0:
    print("Duplicate IDs found:")
    duplicate_ids.show(truncate=False)
else:
    print("All IDs are unique.")

# Query to select all rows from the table
df_all = spark.sql("SELECT * FROM admelakehouse.main")

# Count the number of rows
num_documents = df_all.count()

# Print the number of documents
print(f"Number of documents in admelakehouse.main: {num_documents}")

# Query to select all rows from the table
df_all_run = spark.sql("SELECT * FROM admelakehouse.run_info")

# Count the number of rows
num_documents_run = df_all_run.count()

# Print the number of documents
print(f"Number of documents in admelakehouse.run_info: {num_documents_run}")

