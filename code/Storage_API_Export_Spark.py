#!/usr/bin/env python
# coding: utf-8

# ## Storage_API_Export
#
# Export of Azure Data Manager for Energy (ADME) data to Fabric
#
# External dependencies:
# - ADME instance
# - Azure KeyVault to securely store the secret for connecting to ADME
#
# Delta Tables for logging, data, and last-run info have been set up in a separate notebook.
#
# Setup variables for ADME (these are accessed via config["variable"]).

import pandas as pd
import json
import os

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
    "main_table": "storage_records_bronze_5",
    "logging_table": "_info",
    "run_info_table": "_run"
}
'''

# Load configuration
config_dict = json.loads(config_json)
config = pd.Series(config_dict)

# For security, if key_vault_name and secret_name are provided, retrieve the secret from Azure Key Vault.
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
from datetime import datetime
import uuid
import inspect
from threading import Lock

log_batch = []
log_lock = Lock()

def log_message(level, message):
    global log_batch
    # Log immediately to console and Python logging
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
        print(f"DEBUG: {message}")
        logging.debug(message)

    # Create and add a log entry for later writing to Delta
    log_id = str(uuid.uuid4())
    frame = inspect.currentframe().f_back
    file_name = frame.f_code.co_filename
    line_number = frame.f_lineno
    log_timestamp = datetime.utcnow()
    log_entry = (log_id, log_timestamp, level.upper(), file_name, str(line_number), message)
    with log_lock:
        log_batch.append(log_entry)

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
    print("INFO: Logging finished")

# -------------------------
# Schema Definitions
# -------------------------
from pyspark.sql.types import StructType, StructField, StringType, MapType, TimestampType

# Data schema for Storage API export
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

# Construct authority and authenticate
authority_full = f"{config['authority']}/{config['tenant_id']}"
access_token = authenticate_osdu(
    client_id=config['client_id'],
    client_secret=config['client_secret'],
    authority=authority_full,
    scopes=config['scopes']
)

# -------------------------
# Table Creation (Delta)
# -------------------------
from delta.tables import DeltaTable

def recreate_table_with_new_schema(table_name, table_schema):
    table_path = f"Tables/{table_name}"
    print(f"Processing table: {table_name}, Path: {table_path}")
    table_exists = DeltaTable.isDeltaTable(spark, table_path)
    print(f"Table exists: {table_exists}, Path: {table_path}")
    empty_df = spark.createDataFrame([], table_schema)
    empty_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(table_path)
    print(f"Table {table_name} created or updated at {table_path} with the new schema.")

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
# Reset Last Run Timestamp (for testing)
# -------------------------
from pyspark.sql.functions import current_timestamp, lit, unix_timestamp
import uuid
from datetime import timezone

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {run_info_table} (
    run_id STRING,
    run_timestamp LONG
)
"""
spark.sql(create_table_sql)
spark.sql(f"DELETE FROM {run_info_table}")

test_run_date = "2024-09-05 00:00:00"
run_id = str(uuid.uuid4())
run_info_df = spark.createDataFrame([(run_id,)], ["run_id"])
run_info_df = run_info_df.withColumn("run_timestamp", (unix_timestamp(lit(test_run_date)).cast("long") * 1000000))
print(run_info_df.collect()[0])
run_info_df.write.insertInto(run_info_table, overwrite=False)
last_run_df = spark.sql(f"SELECT run_timestamp FROM {run_info_table} ORDER BY run_timestamp DESC LIMIT 1")
last_run_timestamp = last_run_df.collect()[0]['run_timestamp']
print(f"Last run timestamp: {last_run_timestamp}")
human_readable_timestamp = datetime.fromtimestamp(last_run_timestamp / 1000000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
print(f"Last run timestamp: {human_readable_timestamp}")

# -------------------------
# Main Data Pipeline Functions
# -------------------------
from pyspark.sql.functions import to_timestamp, to_json, col, current_timestamp
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from threading import Thread
from queue import Queue

# Global variables for buffering and thread-safety
write_lock = Lock()
buffer_df = None
write_queue = Queue()

start_time = datetime.now()

# Fetch Storage API data in batches, limiting to 4 concurrent API calls
from concurrent.futures import ThreadPoolExecutor, as_completed
MAX_CONCURRENT_API_CALLS = 4

def fetch_storage_data_in_batches(ids, access_token, batch_size=20, max_workers=MAX_CONCURRENT_API_CALLS):
    processed_count = 0
    success = True
    search_type = config["storage_search_type"]
    search_api = config["storage_url"]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]
            storage_query = {"records": batch_ids}
            futures.append(executor.submit(make_api_call, storage_query, access_token, search_type, search_api))
        for future in as_completed(futures):
            try:
                storage_response = future.result()
                if storage_response:
                    success = process_storage_batch_with_retry(storage_response) and success
                    processed_count += len(storage_response.get('records', []))
                    if processed_count % 100 == 0:
                        elapsed_time = (datetime.now() - start_time).total_seconds() / 60
                        documents_per_minute = processed_count / elapsed_time if elapsed_time > 0 else 0
                        print(f"Processed {processed_count} documents so far... ({documents_per_minute:.2f} documents per minute)")
                else:
                    print("Failed to fetch data for one of the batches.")
                    success = False
            except Exception as e:
                print(f"Error during batch processing: {e}")
                success = False
    return success

# make_api_call for Storage (calls osdu_search_by_cursor)
def make_api_call(query, access_token, search_type, search_api):
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
                return response
            else:
                log_message("ERROR", f"Unexpected response format: {json.dumps(response)}")
                return None
        else:
            log_message("ERROR", "Empty response from API")
            return None
    except Exception as e:
        log_message("ERROR", f"RequestException: {e}")
        return None

# Buffering and asynchronous writing to Delta Lake
def buffer_and_queue_write(unique_df, buffer_size=2000):
    global buffer_df
    if buffer_df is None:
        buffer_df = unique_df
    else:
        buffer_df = buffer_df.union(unique_df)
    if buffer_df.count() >= buffer_size:
        write_queue.put(buffer_df)
        buffer_df = None

def write_to_delta(df):
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

def async_writer():
    while True:
        df = write_queue.get()
        if df is None:
            break
        write_to_delta(df)

writer_thread = Thread(target=async_writer, daemon=True)
writer_thread.start()

def stop_async_writer():
    global buffer_df
    if buffer_df is not None and buffer_df.count() > 0:
        print(f"Flushing remaining {buffer_df.count()} documents from buffer...")
        write_queue.put(buffer_df)
        buffer_df = None
    write_queue.put(None)
    writer_thread.join()

def process_storage_batch_with_retry(storage_response, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            df = spark.createDataFrame(storage_response['records'], schema)
            unique_df = df.dropDuplicates(["id"])
            if 'createTime' in unique_df.columns:
                unique_df = unique_df.withColumn("createTime", to_timestamp(col("createTime"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
            unique_df = unique_df.withColumn("ingestTime", current_timestamp())
            unique_df = unique_df.withColumn("legal", to_json(col("legal"))) \
                                 .withColumn("acl", to_json(col("acl"))) \
                                 .withColumn("tags", to_json(col("tags"))) \
                                 .withColumn("data", to_json(col("data")))
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
                total_count = response.get('totalCount', len(response.get('results', [])))
                print(f"Total documents found: {total_count}")
                first_call = False
            for doc in response.get('results', []):
                if document_limit and len(ids) >= document_limit:
                    print(f"Reached document limit: {document_limit}")
                    break
                ids.append(doc['id'])
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
    if ids:
        success = fetch_storage_data_in_batches(ids, access_token)
    else:
        print("No IDs extracted from the Search API response")
        success = False
    stop_async_writer()
    return success

# -------------------------
# OSDU Search Function (for both Search and Storage API)
# -------------------------
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

def osdu_search_by_cursor(server: str, search_api: str, access_token: str, partition_id: str, query: dict, search_type: str, max_retries=5):
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
            response.raise_for_status()
            json_response = response.json()
            if 'results' in json_response or 'records' in json_response:
                return json_response
            else:
                log_message("ERROR", f"Invalid response content: {json.dumps(json_response)}")
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
                return None
        except requests.exceptions.ConnectionError as e:
            log_message("ERROR", f"Connection Error: {e}, URL: {full_api}")
            return None
        except requests.exceptions.Timeout as e:
            log_message("ERROR", f"Timeout Error: {e}, URL: {full_api}")
            return None
        except Exception as e:
            log_message("ERROR", f"Unexpected error: {e}, URL: {full_api}")
            return None
    log_message("ERROR", f"Max retries exceeded for {full_api}")
    return None

# -------------------------
# Last Run Timestamp Update
# -------------------------
def update_last_run_timestamp():
    run_id = str(uuid.uuid4())
    from pyspark.sql.functions import unix_timestamp, current_timestamp, lit
    run_info_df = spark.createDataFrame([(run_id,)], ["run_id"])
    run_info_df = run_info_df.withColumn("run_timestamp", (unix_timestamp(lit(current_timestamp())).cast("long") * 1000000))
    # In production, write into the run_info table; for now, we just log the update.
    log_message("INFO", "Last run timestamp updated successfully")

# -------------------------
# Main Process Function
# -------------------------
def main_process(access_token, query, reset_last_run=False, document_limit=None):
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

# -------------------------
# Execute Main Process
# -------------------------
print("Batch export started")

# Define Delta table path for main data
table_path = f"Tables/{main_table}"

# Get last run date (for testing, defaulting to 0)
last_run_date = 0
print(f"Last run date (epoch): {last_run_date}")
human_readable_timestamp = datetime.fromtimestamp(last_run_date / 1000000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
print(f"Last run timestamp: {human_readable_timestamp}")

# Define the initial query using the last run date
query = {
    "kind": "osdu:wks:master-data--Wellbore:1.0.0",
    "query": f"version:[{last_run_date} TO *]",
    "limit": batch_size
}

# Execute main process (example: limit to 500 documents for testing)
main_process(access_token=access_token, query=query, reset_last_run=True, document_limit=500)
