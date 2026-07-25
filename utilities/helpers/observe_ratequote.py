import aiohttp  # pyright: ignore[reportMissingImports]
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

async def execute_query(start_time, end_time, rate_quote_id): 
    creds = get_credentials("NA")
    base_url = f"https://{creds['customer_id']}.{creds['domain']}/v1/"
    
    headers = {
        "Authorization": f"Bearer {creds['customer_id']} {creds['access_key']}",
        "Content-Type": "application/json",
        "Accept": "application/x-ndjson"
    }

    pipeline = (
        "filter type = 'CLIENT_RESPONSE' | "
        f"filter body~'{rate_quote_id}' | "
        "pick_col timestamp, sleuthTraceId, body | "
        "sort asc(timestamp)"
    )

    query_data = {
        "query": {
            "outputStage": "myStage",
            "stages": [
                {
                    "input": [
                        {
                            "inputName": "main",
                            "datasetId": creds["dataset_id"]
                        }
                    ],
                    "stageID": "myStage",
                    "pipeline": pipeline
                }
            ]
        },
        "rowCount": "10000"
    }

    start_time_iso = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time_iso = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    url = f"{base_url}meta/export/query?startTime={start_time_iso}&endTime={end_time_iso}&paginate=true"
    
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=query_data, timeout=240) as response:
                response_text = await response.text()
                
                if response.status == 401:
                    logging.error(f"\nAuthentication failed. Error: {response_text}")
                    raise Exception("Authentication failed with Observe API")
                
                if response.status == 400:
                    logging.error(f"Bad request error: {response_text}")
                    return None
                
                if response.status == 202:
                    cursor_id = response.headers.get('X-Observe-Cursor-Id')
                    page_url = f"{base_url}meta/export/query/page?cursorId={cursor_id}&offset=0&numRows=10000"
                    
                    retry_count = 0
                    max_retries = 60
                    wait_time = 2
                    
                    while retry_count < max_retries:
                        async with session.get(page_url, headers=headers, timeout=240) as page_response:
                            
                            if page_response.status == 200:
                                response_text = await page_response.text()
                                lines = response_text.splitlines()
                                
                                results = []  # Store all results
                                if not lines:
                                    return results
                                
                                for line_num, line in enumerate(lines):
                                    if line.strip():
                                        try:
                                            entry = json.loads(line)
                                            if entry.get('sleuthTraceId'):
                                                results.append(entry)  # Append the entire entry
                                        except json.JSONDecodeError as e:
                                            logging.error(f"\nError parsing line {line_num + 1}: {e}")
                                            continue
                                
                                return results  # Return all results found
                            
                            elif page_response.status == 202:
                                logging.info(f"\nRequest still processing, retry {retry_count + 1}/{max_retries}")
                                await asyncio.sleep(wait_time)
                                retry_count += 1
                                continue
                            
                            else:
                                logging.info(f"\nUnexpected page response status: {page_response.status}")
                                error_text = await page_response.text()
                                logging.info(f"Error response content: {error_text}")
                                return None
                    
                    logging.info("\nMaximum retries reached")
                    return None
    
    except aiohttp.ClientError as e:
        logging.error(f"\nRequest error: {str(e)}")
        return None
    except Exception as e:
        logging.error(f"\nUnexpected error: {str(e)}")
        raise

async def fetch_ltl_quote_data(rate_quote_id): 
    if not rate_quote_id:
        raise ValueError("rate_quote_id must be provided")

    total_start_time = time.time()
    logging.info("\n=== Querying Observe API for LTL Quote ===")
    
    # First attempt
    now = datetime.datetime.now(timezone.utc)
    end_time = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    start_time = (now - datetime.timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Initialize response data
    response_data = {
        "rate_quote_id": rate_quote_id,
        "observe-ratequote-filter-url": None
    }

    # Try first 30 days
    logging.info(f"\nFirst attempt - Querying data from {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    quote_results = await process_time_range(start_time, end_time, rate_quote_id)  # Remove instance parameter

    # If no results, try previous 30 days
    if not quote_results:
        previous_end_time = start_time.replace(hour=23, minute=59, second=59, microsecond=999999)  # Change this
        previous_start_time = (previous_end_time - datetime.timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        logging.info(f"\nSecond attempt - Querying data from {previous_start_time.strftime('%Y-%m-%d %H:%M:%S')} to {previous_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        quote_results = await process_time_range(previous_start_time, previous_end_time, rate_quote_id)

    # Process results (if any found in either attempt)
    if quote_results:
        logging.info("\nProcessing quote results...")
        first_valid_result = next((result for result in quote_results if result.get('sleuthTraceId')), None)
        
        if first_valid_result:
            sleuth_trace_id = first_valid_result.get('sleuthTraceId')
            rate_quote_timestamp = first_valid_result.get('timestamp')
            
            if rate_quote_timestamp:
                try:
                    timestamp_seconds = float(rate_quote_timestamp) / 1e9
                    url_start_time = datetime.datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
                    
                    logging.info(f"\nURL Start Time: {url_start_time.strftime('%Y-%m-%dT%H:%M:%S')}")
                    logging.info(f"URL End Time: {end_time.strftime('%Y-%m-%dT%H:%M:%S')}")
                    logging.info(f"sleuth: {sleuth_trace_id}")
                    
                    creds = get_credentials("NA")
                    observe_url = (
                        f"https://{creds['customer_id']}.{creds['domain']}/workspace/{creds['instance_id']}/log-explorer?"
                        f"datasetId={creds['dataset_id']}&"
                        f"filter-sleuthTraceId={sleuth_trace_id}&"
                        f"time-start={url_start_time.strftime('%Y-%m-%dT%H.%M.%S')}%2B05.30&"
                        f"time-end={end_time.strftime('%Y-%m-%dT%H.%M.%S')}%2B05.30"
                    )
                    
                    response_data["observe-ratequote-filter-url"] = observe_url
                    logging.info(f"\nGenerated Observe URL: {observe_url}")
                except Exception as e:
                    logging.error(f"\nError converting timestamp for URL: {e}")
    else:
        logging.info("\nNo results found with sleuthTraceId in either 60-day period")
    
    print_elapsed_time(total_start_time, "\nTotal execution time")
    return response_data

# New helper function to process a time range
async def process_time_range(start_time, end_time, rate_quote_id): 
    # Process in 12-hour chunks
    chunk_delta = datetime.timedelta(hours=24)
    chunks = []
    
    current_chunk_start = start_time
    while current_chunk_start < end_time:
        current_chunk_end = min(current_chunk_start + chunk_delta, end_time)
        chunks.append((current_chunk_start, current_chunk_end))
        current_chunk_start = current_chunk_end
    
    logging.info(f"Will process all {len(chunks)} chunks in parallel")
    
    # Process all chunks in parallel
    tasks = [
        execute_query(chunk_start, chunk_end, rate_quote_id)
        for chunk_start, chunk_end in chunks
    ]
    
    results = await asyncio.gather(*tasks)
    
    quote_results = []
    for chunk_result in results:
        if chunk_result:
            quote_results.extend(chunk_result)
            
    return quote_results