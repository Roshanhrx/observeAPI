import aiohttp
import asyncio
import json
import datetime
import logging
import time
from datetime import timezone

from utilities.helpers.observe_config import get_credentials


def print_elapsed_time(start_time, message):
    elapsed = time.time() - start_time
    logging.info(f"{message}: {elapsed:.2f} seconds")

async def execute_query(start_time, end_time, page_size=10000, error_id=None, instance="NA"):
    query_start_time = time.time()
    results = []

    # Define query pipeline for error logs
    pipeline = (
        f"filter body ~ '{error_id}' and "
        "type = 'SERVER_RESPONSE' | "
        "pick_col timestamp, sleuthTraceId, body | "
        "sort asc(timestamp)"
    )

    # Get credentials based on instance
    creds = get_credentials(instance)
    
    # Create dynamic base_url and headers (only place they're needed)
    base_url = f"https://{creds['customer_id']}.{creds['domain']}/v1/"
    headers = {
        "Authorization": f"Bearer {creds['customer_id']} {creds['access_key']}",
        "Content-Type": "application/json",
        "Accept": "application/x-ndjson"
    }
    
    query_data = {
        "query": {
            "outputStage": "myStage",
            "stages": [
                {
                    "input": [
                        {
                            "inputName": "main",
                            "datasetId": creds['dataset_id']  # Use dynamic dataset_id
                        }
                    ],
                    "stageID": "myStage",
                    "pipeline": pipeline
                }
            ]
        },
        "rowCount": str(page_size)
    }

    # Format dates for the API call
    start_time_iso = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time_iso = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    url = f"{base_url}meta/export/query?startTime={start_time_iso}&endTime={end_time_iso}&paginate=true"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=query_data, timeout=240) as response:
                if response.status == 401:
                    error_text = await response.text()
                    logging.error(f"\nAuthentication failed. Error: {error_text}")
                    raise Exception("Authentication failed with Observe API")
                
                if response.status == 400:
                    error_text = await response.text()
                    logging.error(f"Bad request error: {error_text}")
                    return results
                
                if response.status == 202:
                    cursor_id = response.headers.get('X-Observe-Cursor-Id')
                    offset = 0
                    total_rows = None
                    
                    while True:
                        page_url = f"{base_url}meta/export/query/page?cursorId={cursor_id}&offset={offset}&numRows={page_size}"
                        
                        retry_count = 0
                        max_retries = 60
                        wait_time = 2
                        total_wait_time = 0
                        max_wait_time = 180
                        
                        while retry_count < max_retries and total_wait_time < max_wait_time:
                            try:
                                async with session.get(page_url, headers=headers, timeout=240) as page_response:
                                    if page_response.status == 200:
                                        if total_rows is None:
                                            total_rows = int(page_response.headers.get('X-Observe-Total-Rows', '0'))
                                        
                                        lines = (await page_response.text()).splitlines()
                                        for line in lines:
                                            if line.strip():
                                                try:
                                                    entry = json.loads(line)
                                                    results.append(entry)
                                                except json.JSONDecodeError:
                                                    continue
                                        
                                        offset += page_size
                                        if offset >= total_rows:
                                            return results
                                        
                                        break
                                    
                                    elif page_response.status == 202:
                                        await asyncio.sleep(wait_time)
                                        total_wait_time += wait_time
                                        retry_count += 1
                                    
                                    else:
                                        logging.error(f"\nUnexpected status: {page_response.status}")
                                        return results
                            
                            except aiohttp.ClientError as e:
                                logging.error(f"\nRequest error: {str(e)}")
                                await asyncio.sleep(5)
                                total_wait_time += 5
                                retry_count += 1
                        
                        if total_wait_time >= max_wait_time:
                            logging.error(f"\nMaximum wait time reached")
                            break
                
                return results
    
    except aiohttp.ClientError as e:
        logging.error(f"Initial request error: {e}")
        return results

async def process_time_range(start_time, end_time, error_id, instance="NA"):
    chunk_delta = datetime.timedelta(hours=24)
    chunks = []
    
    current_chunk_start = start_time
    while current_chunk_start < end_time:
        current_chunk_end = min(current_chunk_start + chunk_delta, end_time)
        chunks.append((current_chunk_start, current_chunk_end))
        current_chunk_start = current_chunk_end
    
    logging.info(f"Processing {len(chunks)} chunks in parallel")
    
    tasks = [
        execute_query(
            chunk_start, 
            chunk_end, 
            10000, 
            error_id,
            instance
        )
        for chunk_start, chunk_end in chunks
    ]
    
    results = await asyncio.gather(*tasks)
    
    all_results = []
    for chunk_result in results:
        if chunk_result:
            all_results.extend(chunk_result)
            
    return all_results

async def fetch_error_logs(error_id, instance="NA"):
    if not error_id:
        raise ValueError("error_id must be provided")

    total_start_time = time.time()
    logging.info("\n=== Querying Observe API for Error Logs ===")
    
    # Get current time and set time ranges
    now = datetime.datetime.now(timezone.utc)
    end_time = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    # Initialize response structure
    response_data = {
        "error_id": error_id,
        "error_logs_url": None,  # Changed from "observe_url"
        "period_searched": None
    }

    # First 30-day period
    start_time = (now - datetime.timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
    logging.info(f"\nFirst attempt - Querying data from {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = await process_time_range(start_time, end_time, error_id, instance)

    # If no results, try previous 30 days
    if not results:
        previous_end_time = start_time
        previous_start_time = (previous_end_time - datetime.timedelta(days=30))
        
        logging.info(f"\nSecond attempt - Querying data from {previous_start_time.strftime('%Y-%m-%d %H:%M:%S')} to {previous_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        results = await process_time_range(previous_start_time, previous_end_time, error_id, instance)

        # If still no results, try another 30 days back
        if not results:
            final_end_time = previous_start_time
            final_start_time = (final_end_time - datetime.timedelta(days=30))
            
            logging.info(f"\nFinal attempt - Querying data from {final_start_time.strftime('%Y-%m-%d %H:%M:%S')} to {final_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            results = await process_time_range(final_start_time, final_end_time, error_id, instance)

    # Process results if found in any period
    if results:
        logging.info("\nProcessing results...")
        logging.info(f"Number of error log results found: {len(results)}")
        
        # Get the first entry with sleuthTraceId (already sorted by pipeline)
        oldest_result = next((result for result in results 
                            if result.get('sleuthTraceId')), None)
        
        if oldest_result:
            sleuth_trace_id = oldest_result.get('sleuthTraceId')
            timestamp = oldest_result.get('timestamp')
            
            # Add sleuthTraceId to response data
            response_data["sleuth_trace_id"] = sleuth_trace_id
            
            if timestamp:
                try:
                    # Convert nanoseconds to seconds and create datetime
                    timestamp_seconds = float(timestamp) / 1e9
                    url_start_time = datetime.datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
                    
                    # For error logs URL: use 1-day range from error timestamp
                    url_end_time = url_start_time + datetime.timedelta(days=1)
                    
                    # Get credentials for URL generation
                    creds = get_credentials(instance)
                    observe_url = (
                        f"https://{creds['customer_id']}.{creds['domain']}/workspace/{creds['instance_id']}/log-explorer?"
                        f"datasetId={creds['dataset_id']}&"
                        f"filter-sleuthTraceId={sleuth_trace_id}&"
                        f"time-start={url_start_time.strftime('%Y-%m-%dT%H.%M.%S')}%2B05.30&"
                        f"time-end={url_end_time.strftime('%Y-%m-%dT%H.%M.%S')}%2B05.30"
                    )
                    
                    response_data["error_logs_url"] = observe_url
                    response_data["period_searched"] = "found"
                    logging.info(f"\nGenerated Observe URL: {observe_url}")
                
                except Exception as e:
                    logging.error(f"\nError generating URL: {e}")
        else:
            logging.info("\nNo results with sleuthTraceId found")
            response_data["period_searched"] = "no_trace_id"

    print_elapsed_time(total_start_time, "\nTotal execution time")
    
    # Log the final response data
    logging.info("\nFinal Response Data:")
    logging.info(json.dumps(response_data, indent=2))
    
    return response_data
