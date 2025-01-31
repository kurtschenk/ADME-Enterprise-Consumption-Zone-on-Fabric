#!/usr/bin/env python
# coding: utf-8

# ## Storage_API_Export
# 
# New notebook

# # Export of Azure Data Manager for Energy (ADME) data to Fabric
# 
# ## External dependencies
# ADME instance
# Azure KeyVault to keep secret needed to connect to ADME 
# 
# ## Setup of Delta Tables
# A separate Notebook called "ADME setup and testing" has the code to create the delta lake tables for logging, data and last run info
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
# 
# 
# 
# 

# In[ ]:


import pandas as pd
import json

# Correct the JSON string format and load it
config_json = '''
{
    "server": "https://<adme instance>.energy.azure.com",
    "crs_catalog_url": "/api/crs/catalog/v2/",
    "crs_converter_url": "/api/crs/converter/v2/",
    "entitlements_url": "/api/entitlements/v2/",
    "file_url": "/api/file/v2/",
    "legal_url": "/api/legal/v1/",
    "schema_url": "/api/schema-service/v1/",
    "search_url": "/api/search/v2/",
    "storage_url": "/api/storage/v2/",
    "storage_search_type":"query/records:batch",
    "search_api_search_type":"query_with_cursor",
    "unit_url": "/api/unit/v3/",
    "workflow_url": "/api/workflow/v1/",
    "data_partition_id": "",
    "legal_tag": "",
    "acl_viewer": "",
    "acl_owner": "",
    "authentication_mode": "msal_interactive",
    "authority": "https://login.microsoftonline.com/3aa4a235-b6e2-48d5-9195-7fcf05b459b0",
    "scopes": [""],
    "client_id": "",
    "tenant_id": "",
    "redirect_uri": "http://localhost:8080",
    "access_token_type" : "",
    "key_vault_name" : "",
    "secret_name" : "",
    "table_name" : "",
    "logging_table" : "",
    "run_info_table" : "",
    "lakehouse_name" : ""
}
'''


# Load the JSON string into a Python dictionary
config_dict = json.loads(config_json)

# Create a Series from the dictionary
config = pd.Series(config_dict)

#The number of documents in each batch. If you increase this you could see error messages about the load being too big
batch_size = 1000

display(config)


# # Spark settings
# In the cell below you can define the spark environment. Go to **Workspaces - Choose your workspace and then Settings** in the upper right corner of the screen. Under Data Engineerin/Data Science you can find pool information. There you can see how many nodes you have availabe and can use in the settings below (maxExecutors, minExecutors, startExecutors).
# 
# ## Other settings to consider 
# The code will run fine without these, but they can be used to fine tune based on your needs
# 
# 
# **spark.conf.set("spark.sql.parquet.vorder.enabled", "true")**
# 
# Enables the vectorized Parquet reader in Apache Spark. This setting enhances the performance of reading Parquet files by processing data in batches, taking advantage of the columnar storage format and reducing the overhead associated with row-by-row processing. This can lead to faster query execution and more efficient CPU usage, making it a valuable configuration for optimizing big data processing workflows in Spark.
# 
# **spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")**
# 
# Enables optimized writing for Delta Lake operations. This optimization can significantly improve write performance by batching small files, reducing I/O operations, and better utilizing resources. It is particularly useful for scenarios involving frequent writes to Delta tables, helping to mitigate issues like the small file problem and ensuring efficient data management.
# 
# **spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")**
# 
# Sets the target bin size for the optimized write feature in Delta Lake to 1 GB. This helps in controlling the size of the output files, improving both write and read performance by reducing the number of small files and achieving more consistent file sizes. This setting is particularly useful in optimizing storage and compute resource utilization in environments like Azure Synapse and Azure Databricks.
# 
# **spark.conf.set("spark.sql.shuffle.partitions", "200")**
# 
# Configures Apache Spark to use 200 partitions during shuffle operations. This setting is crucial for tuning the performance of Spark applications, as it affects how data is distributed and processed across the cluster. Adjusting the number of shuffle partitions can help balance the workload, improve parallelism, and optimize resource utilization based on the specific requirements of your data processing tasks.
# 
# **spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")**
# 
# Enables the auto-compaction feature for Delta tables in Databricks. This feature helps manage the small file problem by automatically combining small files into larger ones, improving both read and write performance, optimizing storage efficiency, and enhancing overall system performance. Auto-compaction is a valuable feature for maintaining the health and performance of Delta Lake tables, especially in environments with frequent data writes.
# 

# # Spark Settings

# In[ ]:


# import packages
from pyspark.sql import SparkSession
import requests
import json
import os
from msal import ConfidentialClientApplication
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, MapType, TimestampType, IntegerType
from pyspark.sql.functions import to_timestamp, to_json, from_json, explode, col, first, current_timestamp
from trident_token_library_wrapper import PyTridentTokenLibrary as tl
import time
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
from pyspark.sql.functions import to_timestamp, to_json, year, month, dayofmonth
from delta import DeltaTable

# Initialize Spark Session
#Changed maxExecutors should be based on your available resources in Fabric
spark = SparkSession.builder \
    .appName("FullLoad") \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.memoryOverhead", "4g") \
    .config("spark.driver.memory", "16g") \
    .config("spark.driver.memoryOverhead", "4g") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "4") \
    .config("spark.dynamicAllocation.maxExecutors", "10") \
    .config("spark.dynamicAllocation.initialExecutors", "5") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.compress", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true")\
    .config("spark.sql.parquet.vorder.enabled", "true")\
    .config("spark.microsoft.delta.optimizeWrite.enabled", "true")\
    .config("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")\
    .config("spark.sql.files.maxPartitionBytes", "512MB") \
    .config("spark.sql.shuffle.partitions", 200) \
    .getOrCreate()



# Configure logging at the beginning
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')


# 
# # Schemas
# The solution uses three schemas. One for logging, one for the exported data and one for information on last export. The schemas are listed in the code cell below.

# In[ ]:


# Define the log schema
log_schema = StructType([
    StructField("log_id", StringType(), False),
    StructField("log_timestamp", TimestampType(), False),
    StructField("log_level", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("line_number", StringType(), False),
    StructField("message", StringType(), False)
])

schema = StructType([
    StructField("data", MapType(StringType(), StringType()), True),
    StructField("meta", StringType(), True),
    StructField("id", StringType(), True),
    StructField("version", StringType(), True),
    StructField("kind", StringType(), True),
    StructField("acl", MapType(StringType(), StringType()), True),
    StructField("legal", MapType(StringType(), StringType()), True),
    StructField("tags", MapType(StringType(), StringType()), True),
    StructField("createUser", StringType(), True),
    StructField("createTime", StringType(), True)
])


# # Logging
# Below is the schema for logging information and errors in the code to the logging_info table. Example of use: **log_message("INFO", message)**. The logging code will automatically add line number, timestamp and an unique id. The log will also be printed in the notebook.
# 

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, lit
from pyspark.sql.types import TimestampType, StringType, StructType, StructField
import uuid
import inspect
import logging
from threading import Lock
from datetime import datetime

# Initialize log batch and lock
log_batch = []
log_lock = Lock()

# Define log_message function
def log_message(level, message):
    global log_batch
    
    # Log to console immediately
    if level == "INFO":
        print(f"INFO: {message}")
        logging.info(message)
    elif level == "WARNING":
        print(f"WARNING: {message}")
        logging.warning(message)
    elif level == "ERROR":
        print(f"ERROR: {message}")
        logging.error(message)
    else:
        print(f"DEBUG: {message}")
        logging.debug(message)

    # Create log entry
    log_id = str(uuid.uuid4())
    frame = inspect.currentframe().f_back
    file_name = frame.f_code.co_filename
    line_number = frame.f_lineno
    log_timestamp = datetime.utcnow()

    log_entry = (log_id, log_timestamp, level, file_name, str(line_number), message)

    # Add log entry to batch
    with log_lock:
        log_batch.append(log_entry)


# Write all logs to Delta table at the end
def write_log_batch_to_delta():
    global log_batch
    with log_lock:
        if log_batch:
            # Convert log batch to DataFrame
            log_df = spark.createDataFrame(log_batch, schema=log_schema)

            # Define the paths for Delta tables
            table_name = config["logging_table"]
            table_path = f"Tables/{table_name}"

            # Write the logs to the Delta table in one batch
            log_df.write.format("delta").mode("append").save(table_path)

            # Clear the log batch
            log_batch = []
    print("INFO: Logging finished")

# OSDU search function
import time
from requests.exceptions import HTTPError

def osdu_search_by_cursor(server: str, search_api: str, access_token: str, partition_id: str, query: dict, search_type: str, max_retries=5):
    
    search_api = f"{server}{search_api}{search_type}"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "data-partition-id": partition_id,
        "Content-Type": "application/json"
    }

    retry_strategy = Retry(
        total=max_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        backoff_factor=1  # Initial backoff factor (will be multiplied exponentially)
    )

    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    retries = 0
    backoff_time = 1  # Start with 1 second

    while retries < max_retries:
        try:
            #log_message("INFO", f"Sending request to {search_api} with query: {json.dumps(query)} and cursor: {query.get('cursor')}")
            response = session.post(search_api, headers=headers, json=query)
            response.raise_for_status()  # Raises error for bad HTTP responses
            json_response = response.json()

            # Handle both Search API (results) and Storage API (records) responses
            if 'results' in json_response:
                #log_message("INFO", "Search API returned results.")
                return json_response
            elif 'records' in json_response:
                #log_message("INFO", "Storage API returned records.")
                return json_response
            else:
                error_message = f"Invalid response content: {json_response}"
                log_message("ERROR", error_message)
                return None

        except HTTPError as e:
            if response.status_code == 429:
                retries += 1
                log_message("ERROR", f"Received 429 Too Many Requests. Retry {retries}/{max_retries} after {backoff_time} seconds.")
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    backoff_time = int(retry_after)
                else:
                    backoff_time = min(2 ** retries, 60)  # Exponential backoff, cap at 60 seconds
                time.sleep(backoff_time)
            else:
                error_message = f"HTTP Error: {e}, URL: {search_api}, Query: {json.dumps(query)}"
                log_message("ERROR", error_message)
                return None
        except requests.exceptions.ConnectionError as e:
            log_message("ERROR", f"Connection Error: {e}, URL: {search_api}")
            return None
        except requests.exceptions.Timeout as e:
            log_message("ERROR", f"Timeout Error: {e}, URL: {search_api}")
            return None
        except Exception as e:
            log_message("ERROR", f"Unexpected error: {e}, URL: {search_api}")
            return None

    log_message("ERROR", f"Max retries exceeded for {search_api}")
    return None


# # Authentication

# In[ ]:


from msal import ConfidentialClientApplication
# Authentication function
def authenticate_osdu(client_id: str, client_secret: str, authority: str, scopes: list):
    try:
        app = ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=authority
        )

        result = app.acquire_token_for_client(scopes=scopes)
        
        if "access_token" in result:
            message = "Authentication successful"
            log_message("INFO", message)
            return result['access_token']
        else:
            error_message = f"Authentication failed: {result.get('error')}, {result.get('error_description')}"
            log_message("ERROR", error_message)
    except Exception as e:
        error_message = f"Unexpected error during authentication: {e}"
        log_message("ERROR", error_message)
    return None

key_vault_name = config["key_vault_name"]
access_token = authenticate_osdu(
    client_id = config['client_id'],    
    client_secret= tl.get_secret_with_token(
        f"https://{key_vault_name}.vault.azure.net/",
        config["secret_name"],
        mssparkutils.credentials.getToken(config["access_token_type"])
    ),
    authority= config['authority'],
    scopes= config['scopes']
    
)




# # Creating the table

# In[ ]:


###This code only needs to be run once to establish the table where the data will be stored
rom pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable

# Initialize Spark session
spark = SparkSession.builder \
    .appName("LoggingTableCreation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

# Function to overwrite Delta table schema
def recreate_table_with_new_schema(table_name, schema):
    # Resolve the table path from config
    table_path = f"Tables/{table_name}"
    print(f"Processing table: {table_name}, Path: {table_path}")
    
    # Check if the table exists
    table_exists = DeltaTable.isDeltaTable(spark, table_path)
    print(f"Table exists: {table_exists}, Path: {table_path}")

    # Overwrite the table with the new schema
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(table_path)

    print(f"Table {table_name} created or updated at {table_path} with the new schema.")

# Example usage
# Assuming table_name and storage_schema are loaded from config
table_name = config["table_name"]
storage_schema = StructType([
    StructField("data", StringType(), True),
    StructField("meta", StringType(), True),
    StructField("id", StringType(), True),
    StructField("version", StringType(), True),
    StructField("kind", StringType(), True),
    StructField("acl", StringType(), True),
    StructField("legal", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("createUser", StringType(), True),
    StructField("createTime", TimestampType(), True),
    StructField("ingestTime", TimestampType(), True)
])

# Call the function with table name from config
recreate_table_with_new_schema(table_name, storage_schema)


# # Reset last run time manually

# In[ ]:


## Added code Jon Olav Abeland 24.05. This code will create a table to store last run value for delta
## The purpose of this code is to reset last run time so that it is possible to rerun the code for testing multiple times
## This code can be safely run as it only creates a table if none exists
#from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, unix_timestamp
import uuid
from datetime import datetime, timezone

# Initialize Spark session (uncomment if needed)
# spark = SparkSession.builder.appName("ExampleApp").getOrCreate()

lakehouse_name = config["lakehouse_name"]
table_name = f"{lakehouse_name}.run_info"

# SQL to create the table if it doesn't exist
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {lakehouse_name}.run_info (
    run_id STRING,
    run_timestamp LONG
)
"""

# Execute the SQL to create the table
spark.sql(create_table_sql)

# Clear the table for testing purposes (remove this in production)
spark.sql(f"DELETE FROM {lakehouse_name}.run_info")

# Define the specific test date
test_run_date = "2024-09-05 00:00:00"

# Insert the test run date as epoch timestamp
run_id = str(uuid.uuid4())
run_info_df = spark.createDataFrame([(run_id,)], ["run_id"])
# Convert to epoch time in milliseconds by multiplying the unix timestamp by 1000
run_info_df = run_info_df.withColumn("run_timestamp", (unix_timestamp(lit(test_run_date)).cast("long")*1000000))
print(run_info_df.collect()[0])

run_info_df.write.insertInto(table_name, overwrite=False)

# Query to get the last run value
last_run_df = spark.sql(f"""
SELECT run_timestamp
FROM {lakehouse_name}.run_info
ORDER BY run_timestamp DESC
LIMIT 1
""")

# Collect the result and extract the timestamp
last_run_timestamp = last_run_df.collect()[0]['run_timestamp']

print(f"Last run timestamp: {last_run_timestamp}")


# Convert epoch time back to human-readable format
# Assuming last_run_timestamp is in milliseconds
#human_readable_timestamp = datetime.utcfromtimestamp(last_run_timestamp / 1000000).strftime('%Y-%m-%d %H:%M:%S')
human_readable_timestamp = datetime.fromtimestamp(last_run_timestamp / 1000000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
print(f"Last run timestamp: {human_readable_timestamp}")


# # Main code
# 
# ## Performance
# .config("spark.sql.files.maxPartitionBytes", "512MB") is set in order to make fewer and larger parquet files to improve performance when using the data later, and possibly improve performance for the upcert of new data with my code. 
# 
# The Search API takes 1000 documents in each request in about one second, then ids from this are used to in Storage API in batches of 20 before the data from the Storage API is written to the DataLake. If I run this in order it takes about one minute to load 100 documents. 
# Since the Search API is fast, this code is not optimized. In order to avoid writing the Storage metadata to the DeltaLake 20 documents at a time, there is a buffer that will only write to the DeltaLake once 1000 documents are collected. The writing is also done separately from the Storage API processes. These changes save a lot of time and are about three times faster. 
# 
# The code uses threads in order to call the Storage API in parallell. It seems like there is not much performance improvement beyond 4 processess. This is probably due to the number of nodes available and/or the Storage API forcing the code to back off due to 429 (too many requests). I suggest that 4 threads should be used for the code. The reason that "Writing to Delta" is 0 seconds in the image below for 4 and 8 threads is that it is able to write in parallell with the Storage API calls and do not add time. 
# 

# In[ ]:


from pyspark import SparkContext, SparkConf
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime, timezone
from delta import DeltaTable
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import unix_timestamp, current_timestamp, lit
import uuid
from threading import Thread
from queue import Queue

import time
from concurrent.futures import ThreadPoolExecutor

# Global buffer and write lock
write_lock = Lock()
buffer_df = None  # To accumulate DataFrames
write_queue = Queue()  # Queue for buffered writes

# Fetch and process Storage API data in batches
from concurrent.futures import ThreadPoolExecutor, as_completed
# Fetch and process Storage API data in parallel batches
def fetch_storage_data_in_batches(ids, access_token, batch_size=20, max_workers=2):
    processed_count = 0
    not_found_ids = []
    success = True
    search_type = config["storage_search_type"]
    search_api = config["storage_url"]

    # Executor to run Storage API calls in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        # Submit Storage API calls to be processed in parallel
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]
            storage_query = {"records": batch_ids}

            # Schedule the storage API call
            futures.append(executor.submit(make_api_call, storage_query, access_token, search_type, search_api))

        # As each future completes, process the results
        for future in as_completed(futures):
            try:
                storage_response = future.result()
                if storage_response:
                    # Process the batch when the Storage API call completes
                    success = process_storage_batch_with_retry(storage_response) and success

                    # Update processed count and print every 100 documents
                    processed_count += len(storage_response['records'])
                    if processed_count % 100 == 0:
                        print(f"Processed {processed_count} documents so far...")
                else:
                    print(f"Failed to fetch data for one of the batches.")
                    success = False
            except Exception as e:
                print(f"Error during batch processing: {e}")
                success = False

    if not_found_ids:
        print(f"IDs not found in Storage API: {', '.join(not_found_ids)}")

    return success

# Function to make the API call
def make_api_call(query, access_token, search_type, search_api):
    try:
        #log_message("INFO", f"Making API call with payload: {json.dumps(query)}")
        response = osdu_search_by_cursor(
            server=config['server'],
            search_api=search_api,
            access_token=access_token,
            partition_id=config['data_partition_id'],
            query=query,
            search_type=search_type
        )
        if response:
            if 'results' in response:
                return response
            elif 'records' in response:
                return response
            else:
                log_message("ERROR", f"Unexpected response format: {json.dumps(response)}")
                return None
        else:
            log_message("ERROR", "Empty response from API")
            return None
    except requests.exceptions.RequestException as e:
        log_message("ERROR", f"RequestException: {e}")
        if e.response:
            log_message("ERROR", f"HTTP Error: {e.response.status_code} - {e.response.text}")
        return None

# Function to buffer data and trigger async write when buffer is full
def buffer_and_queue_write(unique_df, buffer_size=2000):
    global buffer_df
    if buffer_df is None:
        buffer_df = unique_df
    else:
        buffer_df = buffer_df.union(unique_df)

    if buffer_df.count() >= buffer_size:
        # Send buffer to queue for async writing and reset buffer
        write_queue.put(buffer_df)
        buffer_df = None

# Function to write to Delta Lake from queue
def async_writer():
    while True:
        df = write_queue.get()  # Get DataFrame from the queue
        if df is None:
            break  # Sentinel value to exit the thread
        write_to_delta(df)

# Function to actually write to Delta Lake
def write_to_delta(df):
    with write_lock:
        table = DeltaTable.forPath(spark, table_path)
        table.alias("table") \
            .merge(
                df.alias("updates"),
                "table.id = updates.id"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

# Modified process_storage_batch_with_retry to use buffer_and_queue_write
def process_storage_batch_with_retry(storage_response, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            # Create DataFrame from the Storage API response
            df = spark.createDataFrame(storage_response['records'], schema)
            unique_df = df.dropDuplicates(["id"])

            # Apply necessary transformations (createTime and ingestTime)
            if 'createTime' in df.columns:
                unique_df = unique_df.withColumn("createTime", to_timestamp(col("createTime"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

            unique_df = unique_df.withColumn("ingestTime", current_timestamp())
            unique_df = unique_df.withColumn("legal", to_json(col("legal"))) \
                                 .withColumn("acl", to_json(col("acl"))) \
                                 .withColumn("tags", to_json(col("tags"))) \
                                 .withColumn("data", to_json(col("data")))

            # Buffer the data and queue for async writing
            buffer_and_queue_write(unique_df)

            return True
        except AnalysisException as e:
            if "concurrent update" in str(e):
                retries += 1
                print(f"Concurrent update detected. Retry {retries} of {max_retries}.")
                time.sleep(2 ** retries)
            else:
                print(f"Failed to process batch: {e}")
                break
    print(f"Failed to process batch after {max_retries} retries")
    return False

# Async writer thread to handle Delta writes
writer_thread = Thread(target=async_writer, daemon=True)
writer_thread.start()

# After all API calls, flush any remaining data in the buffer and stop the writer
def stop_async_writer():
    global buffer_df
    # Check if there are any remaining records in the buffer
    if buffer_df is not None and buffer_df.count() > 0:
        print(f"Flushing remaining {buffer_df.count()} documents from buffer...")
        write_queue.put(buffer_df)  # Flush remaining data in the buffer
        buffer_df = None  # Reset the buffer

    # Signal the async writer to stop by adding a sentinel value
    write_queue.put(None)
    writer_thread.join()


# Fetch and process documents with a document limit
def sequential_api_calls_with_parallel_processing(cursor, access_token, query, document_limit=None):
    ids = []
    total_count = 0
    first_call = True
    search_type = config["search_api_search_type"]
    search_api = config["search_url"]

    while True:
        response = make_api_call(query, access_token, search_type, search_api)
        if response:
            if first_call:
                total_count = response.get('totalCount', len(response['results']))
                print(f"Total documents found: {total_count}")
                first_call = False

            for doc in response['results']:
                if document_limit and len(ids) >= document_limit:
                    print(f"Reached document limit: {document_limit}")
                    break
                ids.append(doc['id'])

            # Stop processing if document limit is reached
            if document_limit and len(ids) >= document_limit:
                break

            cursor = response.get('cursor')
            if cursor:
                query['cursor'] = cursor
            else:
                print(f"All pages processed. Total documents processed: {len(ids)}")
                break
        else:
            print("Search API call failed!")
            return False

    # Process fetched IDs in batches
    if ids:
        success = fetch_storage_data_in_batches(ids, access_token)
    else:
        print("No IDs extracted from the Search API response")
        success = False

    # Stop the async writer after all processing
    stop_async_writer()

    return success

# Main code
print("Batch export started")

# Define paths for Delta tables
table_name = config["table_name"]
table_path = f"Tables/{table_name}"
lakehouse_name = config["lakehouse_name"]

# Query to get the last run value
last_run_df = spark.sql(f"""
SELECT run_timestamp
FROM {lakehouse_name}.run_info
ORDER BY run_timestamp DESC
LIMIT 1
""")

# Check if the last run timestamp exists
if last_run_df.count() == 0:
    last_run_date = 0  # Default to epoch if no previous run
else:
    last_run_date = last_run_df.collect()[0]['run_timestamp']

###last_run_date is epoch/version
query = {
    "kind": "*:*:*:*",
    "query": f"version:[{last_run_date} TO *]",
    "limit": batch_size
}

# Set a document limit for testing performance
document_limit = 2000  # Set this to the number of documents you'd like to test

search_type = config["search_api_search_type"]
search_api = config["search_url"]


print(f"Last run date (epoch): {last_run_date}")
human_readable_timestamp = datetime.fromtimestamp(last_run_date / 1000000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
print(f"Last run timestamp: {human_readable_timestamp}")

# Function to update the last run timestamp in the run_info table
def update_last_run_timestamp(lakehouse_name):
    # Delete existing run_info records to keep only the latest run
    #spark.sql(f"DELETE FROM {lakehouse_name}.run_info")

    # Insert the current timestamp in epoch format as the new last run timestamp
    run_id = str(uuid.uuid4())
    run_info_df = spark.createDataFrame([(run_id,)], ["run_id"])
    run_info_df = run_info_df.withColumn("run_timestamp", (unix_timestamp(lit(current_timestamp())).cast("long") * 1000000))
    #run_info_df.write.insertInto(f"{lakehouse_name}.run_info", overwrite=False)
    log_message("INFO", "Last run timestamp updated successfully")

# Function to process the main data pipeline
def main_process(access_token, query, reset_last_run=False, document_limit=None):
    # Make the initial API call to get the cursor and total count
    response = make_api_call(query, access_token, search_type, search_api)
    
    if response:
        cursor = response.get('cursor')
        total_count = response.get('totalCount', 0)
        log_message("INFO", f"Documents found: {total_count}")

        # Start processing batches sequentially with document limit
        success = sequential_api_calls_with_parallel_processing(cursor, access_token, query, document_limit=document_limit)

        if success:
            lakehouse_name = config["lakehouse_name"]

            # Reset last run timestamp if required
            if reset_last_run:
                log_message("INFO", "Resetting the last run timestamp...")
                update_last_run_timestamp(lakehouse_name)
            else:
                # Update the last run timestamp if processing was successful
                log_message("INFO", "Updating the last run timestamp after successful processing...")
                update_last_run_timestamp(lakehouse_name)
        else:
            log_message("ERROR", "Batch processing failed. Last run timestamp not updated.")
    else:
        log_message("ERROR", "Initial API call failed.")

    # Ensure the logging process completes
    write_log_batch_to_delta()
    log_message("INFO", "write_log_batch_to_delta() executed successfully")

# Assuming access_token, query, and other configurations are set correctly
###make document limin write last run time. Set time value from last document as last run time
main_process(access_token=access_token, query=query, reset_last_run=True, document_limit=20000000000000)  # Example with document limit

