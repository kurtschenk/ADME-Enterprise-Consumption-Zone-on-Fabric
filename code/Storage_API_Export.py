#!/usr/bin/env python
# coding: utf-8

# ## Storage_API_Export
# Export of Azure Data Manager for Energy (ADME) data to Fabric
#
# External dependencies:
#   - ADME instance
#   - Azure KeyVault for secure secret retrieval
#
# Delta tables for logging, data, and last-run info have been created in a separate notebook.
#
# Setup variables (accessed via config["variable"]) include server, API URLs, authentication details, etc.

import pandas as pd
import json
import os
from datetime import datetime, timezone
import uuid
import time

if 'cached_response' not in globals():
    cached_response = None


# -------------------------
# Configuration Section
# -------------------------
config_json = '''
{
    "server": "https://mohahuja.energy.azure.com",
    "crs_catalog_url": "/api/crs/catalog/v2/",
    "crs_converter_url": "/api/crs/converter/v2/",
    "entitlements_url": "/api/entitlements/v2/",
    "file_url": "/api/file/v2/",
    "legal_url": "/api/legal/v1/",
    "schema_url": "/api/schema-service/v1/",
    "search_url": "/api/search/v2/",
    "storage_url": "/api/storage/v2/",
    "storage_search_type": "query/records:batch",
    "search_api_search_type": "query_with_cursor",
    "unit_url": "/api/unit/v3/",
    "workflow_url": "/api/workflow/v1/",
    "data_partition_id": "testdata",
    "legal_tag": "legal_tag",
    "acl_viewer": "acl_viewer",
    "acl_owner": "acl_owner",
    "authentication_mode": "msal_interactive",
    "authority": "https://login.microsoftonline.com",
    "scopes": ["https://management.core.windows.net/.default"],
    "client_id": "30f78451-12ec-41f2-8495-89845fca77ce",
    "client_secret": "{{SECRET}}",
    "tenant_id": "e2b70470-e94a-4309-936b-4830585f32e9",
    "redirect_uri": "http://localhost:5050",
    "access_token_type": "",
    "key_vault_name": "",
    "secret_name": "",
    "main_table": "storage_records_bronze_1",
    "logging_table": "_info",
    "run_info_table": "_run"
}
'''

config_dict = json.loads(config_json)
config = pd.Series(config_dict)

# Secure configuration: if key_vault_name and secret_name are provided, retrieve client_secret from Key Vault.
if config["key_vault_name"] and config["secret_name"]:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient
    def get_secret_from_keyvault(key_vault_name, secret_name):
        key_vault_url = f"https://{key_vault_name}.vault.azure.net"
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=key_vault_url, credential=credential)
        return client.get_secret(secret_name).value
    config["client_secret"] = get_secret_from_keyvault(config["key_vault_name"], config["secret_name"])

# Batch size for API calls
batch_size = 1000

display(config)

main_table = config["main_table"]
run_info_table = config["run_info_table"]
logging_table = config["logging_table"]


# -------------------------
# Spark Settings
# -------------------------
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FullLoad") \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.memoryOverhead", "4g") \
    .config("spark.driver.memory", "16g") \
    .config("spark.driver.memoryOverhead", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "4") \
    .config("spark.dynamicAllocation.maxExecutors", "10") \
    .config("spark.dynamicAllocation.initialExecutors", "5") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.compress", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.sql.parquet.vorder.enabled", "true") \
    .config("spark.microsoft.delta.optimizeWrite.enabled", "true") \
    .config("spark.microsoft.delta.optimizeWrite.binSize", "1073741824") \
    .config("spark.sql.files.maxPartitionBytes", "512MB") \
    .getOrCreate()

# -------------------------
# Logging Setup
# -------------------------
import logging
import inspect
from threading import Lock

log_batch = []
log_lock = Lock()

def log_message(level, message):
    global log_batch

    start_time = datetime.now()
    # print("DEBUG: log_message: start")

    # Immediately log to console and via Python logging
    if level.upper() == "INFO":
        print(f"INFO: {message}")
        logging.info(message)
    elif level.upper() == "WARNING":
        print(f"WARNING: {message}")
        logging.warning(message)
    elif level.upper() == "ERROR":
        print(f"ERROR: {message}")
        logging.error(message)
    else:
        # print(f"DEBUG: {message}")
        logging.debug(message)
    # Create a log entry and add to batch
    log_id = str(uuid.uuid4())
    frame = inspect.currentframe().f_back
    file_name = frame.f_code.co_filename
    line_number = frame.f_lineno
    log_timestamp = datetime.utcnow()
    entry = (log_id, log_timestamp, level.upper(), file_name, str(line_number), message)
    with log_lock:
        log_batch.append(entry)

    elapsed = (datetime.now() - start_time).total_seconds() * 1000
    if (elapsed > 2):
        print(f"DEBUG: log_message: end: {elapsed:.2f} ms")


def write_log_batch_to_delta():
    global log_batch
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    log_schema = StructType([
        StructField("log_id", StringType(), False),
        StructField("log_timestamp", TimestampType(), False),
        StructField("log_level", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("line_number", StringType(), False),
        StructField("message", StringType(), False)
    ])
    with log_lock:
        if log_batch:
            log_df = spark.createDataFrame(log_batch, schema=log_schema)
            table_path = f"Tables/{logging_table}"
            log_df.write.format("delta").mode("append").save(table_path)
            log_batch = []
    log_message("INFO", "Logging finished")

# -------------------------
# Schema Definitions
# -------------------------
from pyspark.sql.types import StructType, StructField, StringType, MapType, TimestampType

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

# -------------------------
# Authentication
# -------------------------
from msal import ConfidentialClientApplication

def authenticate_osdu(client_id: str, client_secret: str, authority: str, scopes: list):
    try:
        app = ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=authority
        )
        result = app.acquire_token_for_client(scopes=scopes)
        if "access_token" in result:
            log_message("INFO", "Authentication successful")
            return result['access_token']
        else:
            log_message("ERROR", f"Authentication failed: {result.get('error')}, {result.get('error_description')}")
    except Exception as e:
        log_message("ERROR", f"Unexpected error during authentication: {e}")
    return None

authority_full = f"{config['authority']}/{config['tenant_id']}"
access_token = authenticate_osdu(
    client_id=config['client_id'],
    client_secret=config['client_secret'],
    authority=authority_full,
    scopes=config['scopes']
)

# -------------------------
# Delta Table Creation
# -------------------------
from delta.tables import DeltaTable

def recreate_table_with_new_schema(table_name, table_schema):
    table_path = f"Tables/{table_name}"
    log_message("INFO",f"Processing table: {table_name}, Path: {table_path}")
    table_exists = DeltaTable.isDeltaTable(spark, table_path)
    log_message("INFO", f"Table exists: {table_exists}, Path: {table_path}")
    empty_df = spark.createDataFrame([], table_schema)
    empty_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(table_path)
    log_message("INFO", f"Table {table_name} created or updated at {table_path} with the new schema.")

from pyspark.sql.types import LongType
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
recreate_table_with_new_schema(main_table, storage_schema)

# -------------------------
# Reset Last Run Timestamp (Testing Only)
# -------------------------
from pyspark.sql.functions import current_timestamp, lit, unix_timestamp
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {run_info_table} (
    run_id STRING,
    run_timestamp LONG
)
"""
spark.sql(create_table_sql)
spark.sql(f"DELETE FROM {run_info_table}")
test_run_date  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

run_id = str(uuid.uuid4())
run_info_df = spark.createDataFrame([(run_id,)], ["run_id"])
run_info_df = run_info_df.withColumn("run_timestamp", (unix_timestamp(lit(test_run_date)).cast("long") * 1000000))
log_message("INFO", f"{run_info_df.collect()[0]}")
run_info_df.write.insertInto(run_info_table, overwrite=False)
last_run_df = spark.sql(f"SELECT run_timestamp FROM {run_info_table} ORDER BY run_timestamp DESC LIMIT 1")
last_run_timestamp = last_run_df.collect()[0]['run_timestamp']
log_message("INFO", f"Last run timestamp: {last_run_timestamp}")
human_readable_timestamp = datetime.fromtimestamp(last_run_timestamp / 1000000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
log_message("INFO", f"Last run timestamp: {human_readable_timestamp}")

# -------------------------
# Main Data Pipeline Functions
# -------------------------
from pyspark.sql.functions import to_timestamp, to_json, col, current_timestamp
from pyspark.sql.utils import AnalysisException
from threading import Thread
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed

# Global variables for buffering and thread safety
write_lock = Lock()
buffer_df = None
write_queue = Queue()

start_time = datetime.now()

MAX_CONCURRENT_API_CALLS = 4  # Limit to 4 concurrent Storage API calls

def fetch_storage_data_in_batches(ids, access_token, batch_size=20, max_workers=MAX_CONCURRENT_API_CALLS):
    final_batch = False

    start_time = datetime.now()
    log_message("DEBUG", "fetch_storage_data_in_batches: start")
    
    processed_count = 0
    success = True
    search_type = config["storage_search_type"]
    search_api = config["storage_url"]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]
            storage_query = {"records": batch_ids}
            log_message("DEBUG", "fetch_storage_data_in_batches: futures.append")
            futures.append(executor.submit(make_storage_api_call, storage_query, access_token, search_type, search_api, False))
        for future in as_completed(futures):
            try:
                log_message("DEBUG", "fetch_storage_data_in_batches: futures as_completed")
                storage_response = future.result()
                # storage_response = mock_storage_records
                # print(f"Storage response: {storage_response}")
                if storage_response:
                    if processed_count % 2000 == 0:
                        log_message("DEBUG", f"fetch_storage_data_in_batches: final batch: {processed_count}")
                        final_batch = True   
                    else:
                        final_batch = False
                    success = process_storage_batch_with_retry(storage_response, 3, final_batch) and success
                    processed_count += len(storage_response.get('records', []))
                    if processed_count % 100 == 0:
                        elapsed = (datetime.now() - start_time).total_seconds() / 60
                        dpm = processed_count / elapsed if elapsed > 0 else 0
                        log_message("INFO", f"Processed {processed_count} documents... ({dpm:.2f} docs per minute)")
                else:
                    log_message("ERROR","Failed to fetch data for a batch.")
                    success = False
            except Exception as e:
                log_message("ERROR", f"Error during batch processing: {e}")
                success = False

    # end_time = datetime.now()
    elapsed = (datetime.now() - start_time).total_seconds() * 1000
    log_message("DEBUG", f"fetch_storage_data_in_batches: end: {elapsed:.2f} ms")
    return success


def make_api_call(query, access_token, search_type, search_api):

    start_time = datetime.now()
    log_message("DEBUG", f"make_api_call, {search_api}: start")

    try:
        response = osdu_search_by_cursor(
            server=config['server'],
            search_api=search_api,
            access_token=access_token,
            partition_id=config['data_partition_id'],
            query=query,
            search_type=search_type
        )
        
        if response:
            if 'results' in response or 'records' in response:
                # end_time = datetime.now()
                elapsed = (datetime.now() - start_time).total_seconds() * 1000
                log_message("DEBUG", f"make_api_call, {search_api}: end: {elapsed:.2f} ms")
                return response
            else:
                log_message("ERROR", f"Unexpected response format: {json.dumps(response)}")
                # end_time = datetime.now()
                elapsed = (datetime.now() - start_time).total_seconds() * 1000
                log_message("DEBUG", f"make_api_call, {search_api}: end: {elapsed:.2f} ms")
                return None
        else:
            log_message("ERROR", "Empty response from API")
            # end_time = datetime.now()
            elapsed = (datetime.now() - start_time).total_seconds() * 1000
            log_message("DEBUG", f"make_api_call, {search_api}: end: {elapsed:.2f} ms")
            return None
    except Exception as e:
        log_message("ERROR", f"Request exception: {e}")
        # end_time = datetime.now()
        elapsed = (datetime.now() - start_time).total_seconds() * 1000
        log_message("DEBUG", f"make_api_call, {search_api}: end: {elapsed:.2f} ms")
        return None


def make_storage_api_call(query, access_token, search_type, search_api, use_cached=False):
    global cached_response  # Use the cached_response from the global scope
    
    start_time = datetime.now()
    log_message("DEBUG", f"make_storage_api_call, {search_api}: start")

     #print (f"use_cached: {use_cached}, cached_response: {cached_response}, query: {query}, search_api: {search_api}")

    # If cached response exists and use_cached is True, return the cached response
    if use_cached and cached_response:
        elapsed = (datetime.now() - start_time).total_seconds() * 1000
        log_message("DEBUG", f"make_storage_api_call, {search_api}: end: cached {use_cached}: {elapsed:.2f} ms")
        return cached_response
    try:
        response = make_api_call(query, access_token, search_type, search_api)
        
        if response:
            if 'results' in response or 'records' in response:
                # Save the response to cache if it's the first valid response
                cached_response = response
                # print(f"cache the response: {cached_response}"[:50])
                elapsed = (datetime.now() - start_time).total_seconds() * 1000
                log_message("DEBUG", f"make_storage_api_call, {search_api}: end: cached {use_cached}: {elapsed:.2f} ms")
                return response
            else:
                log_message("ERROR", f"Unexpected response format: {json.dumps(response)}")
                elapsed = (datetime.now() - start_time).total_seconds() * 1000
                log_message("DEBUG", f"make_storage_api_call, {search_api}: end: cached {use_cached}: {elapsed:.2f} ms")
                return None
        else:
            log_message("ERROR", "Empty response from API")
            elapsed = (datetime.now() - start_time).total_seconds() * 1000
            log_message("DEBUG", f"make_storage_api_call, {search_api}: end: cached {use_cached}: {elapsed:.2f} ms")
            return None
    except Exception as e:
        log_message("ERROR", f"Request exception: {e}")
        elapsed = (datetime.now() - start_time).total_seconds() * 1000
        log_message("DEBUG", f"make_storage_api_call, {search_api}: end: cached {use_cached}: {elapsed:.2f} ms")
        return None

# Global variable to track the number of rows in the buffer
if 'buffer_row_count' not in globals():
    buffer_row_count = 0

# Buffering and asynchronous writing to Delta Lake
def buffer_and_queue_write(unique_df, final_batch: bool):
    global buffer_df, buffer_row_count

    start_time = datetime.now()
    log_message("DEBUG", "buffer_and_queue_write: start")
    
    # TODO: Still expensive
    # Get count of new rows once
    # new_rows = unique_df.count()
    
    # Update the global buffer
    if buffer_df is None:
        buffer_df = unique_df
    else:
        buffer_df = buffer_df.union(unique_df)
        elapsed = (datetime.now() - start_time).total_seconds() * 1000    
        log_message("DEBUG", f"buffer_and_queue_write: add rows to dataframe: {elapsed:.2f} ms")   
        
    # Check against the buffer size using the running counter
    # if buffer_row_count >= buffer_size:
    if final_batch:
        elapsed = (datetime.now() - start_time).total_seconds() * 1000    
        log_message("DEBUG", f"buffer_and_queue_write: final_batch: {final_batch}: before write_queue.put: {elapsed:.2f} ms")
        write_queue.put((buffer_df, datetime.now()))
        elapsed = (datetime.now() - start_time).total_seconds() * 1000    
        log_message("DEBUG", f"buffer_and_queue_write: after write_queue.put: {elapsed:.2f} ms")
        buffer_df = None
        buffer_row_count = 0  # Reset counter
    
    elapsed = (datetime.now() - start_time).total_seconds() * 1000
    log_message("DEBUG", f"buffer_and_queue_write: end: {elapsed:.2f} ms")


def write_to_delta(df):
    start_time = datetime.now()
    log_message("DEBUG", "write_to_delta: start")

    with write_lock:
        table = DeltaTable.forPath(spark, f"Tables/{main_table}")
        table.alias("table") \
            .merge(
                df.alias("updates"),
                "table.id = updates.id"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    elapsed = (datetime.now() - start_time).total_seconds() * 1000
    log_message("DEBUG", f"write_to_delta: end: {elapsed:.2f} ms")

def async_writer():
    while True:
        start_time = datetime.now()
        log_message("DEBUG", "async_writer: start")
        df, put_in_queue_time = write_queue.get()
        elapsed = (datetime.now() - put_in_queue_time).total_seconds() * 1000
        log_message("DEBUG", f"async_writer: time in queue: {elapsed:.2f} ms: when put in queue: {put_in_queue_time }")
        if df is None:
            break
        write_to_delta(df)

    elapsed = (datetime.now() - start_time).total_seconds() * 1000
    log_message("DEBUG",f"async_writer: end: {elapsed:.2f} ms")

writer_thread = Thread(target=async_writer, daemon=True)
writer_thread.start()

def stop_async_writer():
    global buffer_df
    if buffer_df is not None and buffer_df.count() > 0:
        log_message("INFO", f"Flushing remaining {buffer_df.count()} documents from buffer...")
        write_queue.put((buffer_df, datetime.now()))
        buffer_df = None
    write_queue.put((None, datetime.now()))
    writer_thread.join()


def process_storage_batch_with_retry(storage_response, max_retries, final_batch: bool):
    start_time = datetime.now()
    log_message("DEBUG", "process_storage_batch_with_retry: start")
  
    retries = 0
    while retries < max_retries:
        try:
            log_message("DEBUG", f"process_storage_batch_with_retry: in try, retries {retries}, final_batch: {final_batch}")  
            df = spark.createDataFrame(storage_response['records'], schema)
            unique_df = df.dropDuplicates(["id"])
            if 'createTime' in unique_df.columns:
                unique_df = unique_df.withColumn("createTime", to_timestamp(col("createTime"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
            unique_df = unique_df.withColumn("ingestTime", current_timestamp())
            unique_df = unique_df.withColumn("legal", to_json(col("legal"))) \
                                 .withColumn("acl", to_json(col("acl"))) \
                                 .withColumn("tags", to_json(col("tags"))) \
                                 .withColumn("data", to_json(col("data")))
            log_message("DEBUG", f"process_storage_batch_with_retry: before buffer_and_queue_write, retries {retries}, final_batch: {final_batch}")  
            buffer_and_queue_write(unique_df, final_batch)
            elapsed = (datetime.now() - start_time).total_seconds() * 1000
            log_message("DEBUG", f"process_storage_batch_with_retry: end: {elapsed:.2f} ms")  
            return True
        except AnalysisException as e:
            if "concurrent update" in str(e):
                retries += 1
                log_message("ERROR", f"Concurrent update detected. Retry {retries} of {max_retries}.")
                time.sleep(2 ** retries)
            else:
                log_message("ERROR", f"Failed to process batch: {e}")
                elapsed = (datetime.now() - start_time).total_seconds() * 1000
                log_message("DEBUG", f"process_storage_batch_with_retry:: end: {elapsed:.2f} ms")  
                break

    log_message("ERROR", f"Failed to process batch after {max_retries} retries")
    elapsed = (datetime.now() - start_time).total_seconds() * 1000
    log_message("DEBUG", f"process_storage_batch_with_retry:: end: {elapsed:.2f} ms")  
    return False

def sequential_api_calls_with_parallel_processing(cursor, access_token, query, document_limit=None):
    start_time = datetime.now()
    log_message("DEBUG", f"sequential_api_calls_with_parallel_processing: start")

    ids = []
    total_count = 0
    first_call = True
    search_type = config["search_api_search_type"]
    search_api = config["search_url"]
    while True:
        response = make_api_call(query, access_token, search_type, search_api)
        if response:
            if first_call:
                total_count = response.get('totalCount', len(response.get('results', [])))
                log_message("INFO", f"Total documents found: {total_count}")
                first_call = False
            for doc in response.get('results', []):
                if document_limit and len(ids) >= document_limit:
                    log_message("INFO", f"Reached document limit: {document_limit}")
                    break
                ids.append(doc['id'])
            if document_limit and len(ids) >= document_limit:
                break
            cursor = response.get('cursor')
            if cursor:
                query['cursor'] = cursor
            else:
                log_message("INFO", f"All pages processed. Total documents: {len(ids)}")
                break
        else:
            log_message("ERROR", "Search API call failed!")
            elapsed = (datetime.now() - start_time).total_seconds() * 1000
            log_message("sequential_api_calls_with_parallel_processing: end: {elapsed:.2f} ms")
            return False
    if ids:
        success = fetch_storage_data_in_batches(ids, access_token)
    else:
        log_message("INFO", "No IDs extracted from the Search API response")
        success = False
    stop_async_writer()
    elapsed = (datetime.now() - start_time).total_seconds() * 1000
    log_message("DEBUG", f"sequential_api_calls_with_parallel_processing: end: {elapsed:.2f} ms")
    return success

# -------------------------
# OSDU Search Function
# -------------------------
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def osdu_search_by_cursor(server: str, search_api: str, access_token: str, partition_id: str, query: dict, search_type: str, max_retries=5):
    start_time = datetime.now()
    log_message("DEBUG", "osdu_search_by_cursor: start")

    full_api = f"{server}{search_api}{search_type}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "data-partition-id": partition_id,
        "Content-Type": "application/json"
    }
    retry_strategy = Retry(
        total=max_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        backoff_factor=1
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    retries = 0
    backoff_time = 1
    while retries < max_retries:
        try:
            response = session.post(full_api, headers=headers, json=query)
            response.status_code=429
            response.raise_for_status()
            json_response = response.json()
            if 'results' in json_response or 'records' in json_response:
                elapsed = (datetime.now() - start_time).total_seconds() * 1000
                log_message("DEBUG", f"osdu_search_by_cursor: end: {elapsed:.2f} ms")
                return json_response
            else:
                log_message("ERROR", f"Invalid response: {json.dumps(json_response)}")
                elapsed = (datetime.now() - start_time).total_seconds() * 1000
                log_message("DEBUG", f"osdu_search_by_cursor: end: {elapsed:.2f} ms")
                return None
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                retries += 1
                log_message("ERROR", f"429 Too Many Requests. Retry {retries}/{max_retries} after {backoff_time} seconds.")
                retry_after = response.headers.get('Retry-After')
                backoff_time = int(retry_after) if retry_after else min(2 ** retries, 60)
                time.sleep(backoff_time)
            else:
                log_message("ERROR", f"HTTP Error: {e}, URL: {full_api}, Query: {json.dumps(query)}")
                elapsed = (datetime.now() - start_time).total_seconds() * 1000
                log_message("DEBUG", f"osdu_search_by_cursor: end: {elapsed:.2f} ms")
                return None
        except requests.exceptions.ConnectionError as e:
            log_message("ERROR", f"Connection Error: {e}, URL: {full_api}")
            elapsed = (datetime.now() - start_time).total_seconds() * 1000
            log_message("DEBUG", f"osdu_search_by_cursor: end: {elapsed:.2f} ms")
            return None
        except requests.exceptions.Timeout as e:
            log_message("ERROR", f"Timeout Error: {e}, URL: {full_api}")
            elapsed = (datetime.now() - start_time).total_seconds() * 1000
            log_message("DEBUG", f"osdu_search_by_cursor: end: {elapsed:.2f} ms")
            return None
        except Exception as e:
            log_message("ERROR", f"Unexpected error: {e}, URL: {full_api}")
            elapsed = (datetime.now() - start_time).total_seconds() * 1000
            log_message("DEBUG", f"osdu_search_by_cursor: end: {elapsed:.2f} ms")
            return None
    log_message("ERROR", f"Max retries exceeded for {full_api}")
    elapsed = (datetime.now() - start_time).total_seconds() * 1000
    log_message("DEBUG", f"osdu_search_by_cursor: end: {elapsed:.2f} ms")
    return None

# -------------------------
# Last Run Timestamp Update
# -------------------------
def update_last_run_timestamp():
    run_id = str(uuid.uuid4())
    from pyspark.sql.functions import unix_timestamp, current_timestamp, lit
    run_info_df = spark.createDataFrame([(run_id,)], ["run_id"])
    run_info_df = run_info_df.withColumn("run_timestamp", (unix_timestamp(lit(current_timestamp())).cast("long") * 1000000))
    log_message("INFO", "Last run timestamp updated successfully")
    # In production, write to the run_info table.

# -------------------------
# Main Process Function
# -------------------------
def main_process(access_token, query, reset_last_run=False, document_limit=None):
    start_time = datetime.now()
    log_message("DEBUG", "main_process: start")

    search_type = config["search_api_search_type"]
    search_api = config["search_url"]
    response = make_api_call(query, access_token, search_type, search_api)
    if response:
        cursor = response.get('cursor')
        total_count = response.get('totalCount', 0)
        log_message("INFO", f"Documents found: {total_count}")
        success = sequential_api_calls_with_parallel_processing(cursor, access_token, query, document_limit=document_limit)
        if success:
            if reset_last_run:
                log_message("INFO", "Resetting the last run timestamp...")
                update_last_run_timestamp()
            else:
                log_message("INFO", "Updating the last run timestamp after successful processing...")
                update_last_run_timestamp()
        else:
            log_message("ERROR", "Batch processing failed. Last run timestamp not updated.")
    else:
        log_message("ERROR", "Initial API call failed.")
    write_log_batch_to_delta()
    log_message("INFO", "write_log_batch_to_delta() executed successfully")
    elapsed = (datetime.now() - start_time).total_seconds() * 1000
    log_message("DEBUG", f"main_process: end: {elapsed:.2f} ms")
    log_message("INFO", "See detailed debug logs in _info table")


# -------------------------
# Execute Main Process
# -------------------------
log_message("INFO","Batch export started")
table_path = f"Tables/{main_table}"
last_run_date = 0  # For testing, default to 0
log_message("INFO", f"Last run date (epoch): {last_run_date}")
human_readable = datetime.fromtimestamp(last_run_date / 1000000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
log_message("INFO", f"Last run timestamp: {human_readable}")

query = {
    "kind": "osdu:wks:master-data--Wellbore:1.0.0",
    "query": f"version:[{last_run_date} TO *]",
    "limit": batch_size
}

# Execute main process (example: limit to 500 documents for testing)
main_process(access_token=access_token, query=query, reset_last_run=True, document_limit=40)