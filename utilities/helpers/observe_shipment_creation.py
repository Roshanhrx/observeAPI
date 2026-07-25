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

async def execute_query(start_time, end_time, page_size=10000, master_shipment_id=None, shipment_identifier=None, instance="NA"):
    creds = get_credentials(instance)
    base_url = f"https://{creds['customer_id']}.{creds['domain']}/v1/"
    
    headers = {
        "Authorization": f"Bearer {creds['customer_id']} {creds['access_key']}",
        "Content-Type": "application/json",
        "Accept": "application/x-ndjson"
    }

    query_start_time = time.time()
    results = []

    # Define query pipeline based on provided identifiers
    if master_shipment_id:
        pipeline = (
            f"filter body ~ '{master_shipment_id}' and "
            "type = 'SERVER_RESPONSE' and "
            "statusCode != 404 and statusCode != 401 and statusCode != 500 and statusCode != 400 and "
            "requestURI != '/api/carriers/v2/tl/shipments/statusUpdates' and "
            "requestURI != '/api/v4/capacityproviders/ltl/shipments/statusupdates' and "
            "requestURI != '/api/v4/capacityproviders/tl/shipments/statusUpdates' and "
            "requestURI != '/api/v4/capacityproviders/parcel/shipments/statusupdates' | "
            "pick_col timestamp, sleuthTraceId, body | "
            "sort asc(timestamp)"
        )
    else:
        pipeline = (
            f"filter body ~ '{shipment_identifier}' and "
            "type = 'SERVER_RESPONSE' and "
            "statusCode != 404 and statusCode != 401 and statusCode != 500 and statusCode != 400 and "
            "requestURI != '/api/carriers/v2/tl/shipments/statusUpdates' and "
            "requestURI != '/api/v4/capacityproviders/ltl/shipments/statusupdates' and "
            "requestURI != '/api/v4/capacityproviders/tl/shipments/statusUpdates' and "
            "requestURI != '/api/v4/capacityproviders/parcel/shipments/statusupdates' | "
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
                            "datasetId": creds['dataset_id']
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
                                                    if entry.get('sleuthTraceId'):
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

async def process_time_range(start_time, end_time, master_shipment_id=None, shipment_identifier=None, instance="NA"):
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
            master_shipment_id, 
            shipment_identifier,
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

async def get_webhook_pushes(master_shipment_id, start_time, end_time, instance="NA"):
    creds = get_credentials(instance)
    base_url = f"https://{creds['customer_id']}.{creds['domain']}/v1/"
    
    headers = {
        "Authorization": f"Bearer {creds['customer_id']} {creds['access_key']}",
        "Content-Type": "application/json",
        "Accept": "application/x-ndjson"
    }

    query_start_time = time.time()
    results = []

    # Define query pipeline for webhook pushes
    pipeline = (
        f"filter body ~ '{master_shipment_id}' and "
        "type = 'CLIENT_REQUEST' and "
        "applicationName = 'push-processor' | "
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
                            "datasetId": creds['dataset_id']
                        }
                    ],
                    "stageID": "myStage",
                    "pipeline": pipeline
                }
            ]
        },
        "rowCount": "10000"
    }

    # Use the same chunking logic as before
    chunk_delta = datetime.timedelta(hours=24)
    chunks = []
    
    current_chunk_start = start_time
    while current_chunk_start < end_time:
        current_chunk_end = min(current_chunk_start + chunk_delta, end_time)
        chunks.append((current_chunk_start, current_chunk_end))
        current_chunk_start = current_chunk_end
    
    logging.info(f"\nProcessing {len(chunks)} chunks for webhook pushes")
    
    tasks = []
    for chunk_start, chunk_end in chunks:
        start_time_iso = chunk_start.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time_iso = chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        url = f"{base_url}meta/export/query?startTime={start_time_iso}&endTime={end_time_iso}&paginate=true"
        
        tasks.append(execute_query(chunk_start, chunk_end, 10000, master_shipment_id=master_shipment_id, instance=instance))
    
    chunk_results = await asyncio.gather(*tasks)
    
    for result in chunk_results:
        if result:
            results.extend(result)
            
    return results

async def get_server_request_by_sleuth_trace_id(sleuth_trace_id, start_time, end_time, instance="NA"):
    """
    Query Observe API to get SERVER_REQUEST logs for a specific sleuthTraceId
    Uses PHASED search strategy (30-day periods) with 24-hour chunking and early exit
    Searches backwards from shipment creation (oldest first) for optimal performance
    """
    creds = get_credentials(instance)
    base_url = f"https://{creds['customer_id']}.{creds['domain']}/v1/"
    
    headers = {
        "Authorization": f"Bearer {creds['customer_id']} {creds['access_key']}",
        "Content-Type": "application/json",
        "Accept": "application/x-ndjson"
    }

    # Define query pipeline for SERVER_REQUEST with specific sleuthTraceId
    pipeline = (
        f"filter sleuthTraceId = '{sleuth_trace_id}' and "
        "type = 'SERVER_REQUEST' | "
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
                            "datasetId": creds['dataset_id']
                        }
                    ],
                    "stageID": "myStage",
                    "pipeline": pipeline
                }
            ]
        },
        "rowCount": "10000"
    }

    # Calculate total range in days
    total_range = (end_time - start_time).days
    logging.info(f"Total search range: {total_range} days (from {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')})")
    
    # Create 30-day phases, searching from OLDEST to NEWEST (backwards from shipment creation)
    phase_delta = datetime.timedelta(days=30)
    phases = []
    
    current_phase_start = start_time
    while current_phase_start < end_time:
        current_phase_end = min(current_phase_start + phase_delta, end_time)
        phases.append((current_phase_start, current_phase_end))
        current_phase_start = current_phase_end
    
    logging.info(f"Divided into {len(phases)} phases (30-day periods) - will search oldest first with early exit")
    
    # Create async function for each chunk within a phase
    async def process_chunk(chunk_start, chunk_end, chunk_index, total_chunks, cancel_event):
        chunk_results = []
        start_time_iso = chunk_start.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time_iso = chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        url = f"{base_url}meta/export/query?startTime={start_time_iso}&endTime={end_time_iso}&paginate=true"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=query_data, timeout=240) as response:
                    # Check if another chunk already found results
                    if cancel_event.is_set():
                        return chunk_results
                    
                    if response.status == 401:
                        error_text = await response.text()
                        logging.error(f"\nAuthentication failed for SERVER_REQUEST query. Error: {error_text}")
                        return chunk_results
                    
                    if response.status == 400:
                        error_text = await response.text()
                        logging.error(f"Bad request error for SERVER_REQUEST query: {error_text}")
                        return chunk_results
                    
                    if response.status == 202:
                        cursor_id = response.headers.get('X-Observe-Cursor-Id')
                        offset = 0
                        total_rows = None
                        
                        while True:
                            # Check cancellation before each page request
                            if cancel_event.is_set():
                                return chunk_results
                            
                            page_url = f"{base_url}meta/export/query/page?cursorId={cursor_id}&offset={offset}&numRows=10000"
                            
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
                                                        chunk_results.append(entry)
                                                    except json.JSONDecodeError:
                                                        continue
                                            
                                            offset += 10000
                                            if offset >= total_rows:
                                                if chunk_results:
                                                    cancel_event.set()  # Signal other chunks to stop
                                                return chunk_results
                                            
                                            break
                                        
                                        elif page_response.status == 202:
                                            await asyncio.sleep(wait_time)
                                            total_wait_time += wait_time
                                            retry_count += 1
                                        
                                        else:
                                            logging.error(f"\nUnexpected status in SERVER_REQUEST query: {page_response.status}")
                                            return chunk_results
                                
                                except aiohttp.ClientError as e:
                                    logging.error(f"\nRequest error in SERVER_REQUEST query: {str(e)}")
                                    await asyncio.sleep(5)
                                    total_wait_time += 5
                                    retry_count += 1
                            
                            if total_wait_time >= max_wait_time:
                                logging.error(f"\nMaximum wait time reached for SERVER_REQUEST query")
                                return chunk_results
                            
                            if offset >= total_rows:
                                return chunk_results
        
        except asyncio.CancelledError:
            return chunk_results
        except aiohttp.ClientError as e:
            logging.error(f"Initial request error for SERVER_REQUEST query chunk: {e}")
            return chunk_results
        
        return chunk_results
    
    # Process each phase sequentially (with early exit between phases)
    all_results = []
    
    for phase_index, (phase_start, phase_end) in enumerate(phases):
        phase_range_days = (phase_end - phase_start).days
        logging.info(f"\n--- Phase {phase_index + 1}/{len(phases)}: {phase_start.strftime('%Y-%m-%d')} to {phase_end.strftime('%Y-%m-%d')} ({phase_range_days} days) ---")
        
        # Create 24-hour chunks within this phase
        chunk_delta = datetime.timedelta(hours=24)
        chunks = []
        
        current_chunk_start = phase_start
        while current_chunk_start < phase_end:
            current_chunk_end = min(current_chunk_start + chunk_delta, phase_end)
            chunks.append((current_chunk_start, current_chunk_end))
            current_chunk_start = current_chunk_end
        
        logging.info(f"Processing {len(chunks)} chunks (24-hour intervals) in parallel within this phase")
        
        # Create cancellation event for this phase
        cancel_event = asyncio.Event()
        
        # Process all chunks in this phase in PARALLEL
        tasks = [
            asyncio.create_task(process_chunk(chunk_start, chunk_end, chunk_index, len(chunks), cancel_event))
            for chunk_index, (chunk_start, chunk_end) in enumerate(chunks)
        ]
        
        # Wait for all tasks in this phase
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine results from this phase
        phase_results = []
        for result in results:
            if isinstance(result, list) and result:
                phase_results.extend(result)
        
        if phase_results:
            logging.info(f"✓ Found {len(phase_results)} SERVER_REQUEST results in phase {phase_index + 1} - stopping search")
            all_results.extend(phase_results)
            break  # Early exit - stop processing remaining phases
        else:
            logging.info(f"✗ No results in phase {phase_index + 1}, continuing to next phase...")
    
    logging.info(f"\nFound {len(all_results)} SERVER_REQUEST results total")
    return all_results

async def get_push_tracking_updates(shipment_identifiers, start_time, end_time, instance="NA"):
    """
    Query Observe API to get PUSH_TRACKING status updates for shipment identifiers
    """
    creds = get_credentials(instance)
    base_url = f"https://{creds['customer_id']}.{creds['domain']}/v1/"
    
    headers = {
        "Authorization": f"Bearer {creds['customer_id']} {creds['access_key']}",
        "Content-Type": "application/json",
        "Accept": "application/x-ndjson"
    }

    # Build the filter pipeline for multiple shipment identifiers
    body_conditions = ' or '.join([f"body ~ '{identifier}'" for identifier in shipment_identifiers])
    
    # Define query pipeline for PUSH_TRACKING updates
    pipeline = (
        f"filter ({body_conditions}) and "
        "type = 'SERVER_REQUEST' and "
        "(requestURI = '/api/carriers/v2/tl/shipments/statusUpdates' or "
        "requestURI = '/api/v4/capacityproviders/ltl/shipments/statusUpdates' or "
        "requestURI = '/api/v4/capacityproviders/tl/shipments/statusUpdates' or "
        "requestURI = '/api/v4/capacityproviders/parcel/shipments/statusUpdates') | "
        "pick_col timestamp, sleuthTraceId, body, requestURI | "
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
                            "datasetId": creds['dataset_id']
                        }
                    ],
                    "stageID": "myStage",
                    "pipeline": pipeline
                }
            ]
        },
        "rowCount": "10000"
    }

    # Use chunking for large time ranges
    chunk_delta = datetime.timedelta(hours=24)
    chunks = []
    
    current_chunk_start = start_time
    while current_chunk_start < end_time:
        current_chunk_end = min(current_chunk_start + chunk_delta, end_time)
        chunks.append((current_chunk_start, current_chunk_end))
        current_chunk_start = current_chunk_end
    
    logging.info(f"\nProcessing {len(chunks)} chunks for PUSH_TRACKING updates")
    
    all_results = []
    
    for chunk_start, chunk_end in chunks:
        start_time_iso = chunk_start.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time_iso = chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        url = f"{base_url}meta/export/query?startTime={start_time_iso}&endTime={end_time_iso}&paginate=true"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=query_data, timeout=240) as response:
                    if response.status == 401:
                        error_text = await response.text()
                        logging.error(f"\nAuthentication failed for PUSH_TRACKING query. Error: {error_text}")
                        continue
                    
                    if response.status == 400:
                        error_text = await response.text()
                        logging.error(f"Bad request error for PUSH_TRACKING query: {error_text}")
                        continue
                    
                    if response.status == 202:
                        cursor_id = response.headers.get('X-Observe-Cursor-Id')
                        offset = 0
                        total_rows = None
                        
                        while True:
                            page_url = f"{base_url}meta/export/query/page?cursorId={cursor_id}&offset={offset}&numRows=10000"
                            
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
                                                        all_results.append(entry)
                                                    except json.JSONDecodeError:
                                                        continue
                                            
                                            offset += 10000
                                            if offset >= total_rows:
                                                break
                                            
                                            break
                                        
                                        elif page_response.status == 202:
                                            await asyncio.sleep(wait_time)
                                            total_wait_time += wait_time
                                            retry_count += 1
                                        
                                        else:
                                            logging.error(f"\nUnexpected status in PUSH_TRACKING query: {page_response.status}")
                                            break
                                
                                except aiohttp.ClientError as e:
                                    logging.error(f"\nRequest error in PUSH_TRACKING query: {str(e)}")
                                    await asyncio.sleep(5)
                                    total_wait_time += 5
                                    retry_count += 1
                            
                            if total_wait_time >= max_wait_time:
                                logging.error(f"\nMaximum wait time reached for PUSH_TRACKING query")
                                break
                            
                            if offset >= total_rows:
                                break
        
        except aiohttp.ClientError as e:
            logging.error(f"Initial request error for PUSH_TRACKING query: {e}")
            continue
    
    return all_results

def extract_shipment_identifiers(body_str):
    """
    Extract all shipment identifier values from the body JSON
    Returns a set of unique identifier values
    """
    identifiers = set()
    
    try:
        body_json = json.loads(body_str)
        
        # Extract from shipmentIdentifiers array
        if 'shipmentIdentifiers' in body_json and isinstance(body_json['shipmentIdentifiers'], list):
            for identifier in body_json['shipmentIdentifiers']:
                if isinstance(identifier, dict) and 'value' in identifier:
                    value = identifier['value']
                    if value:  # Only add non-empty values
                        identifiers.add(str(value))
            logging.info(f"Extracted {len(identifiers)} unique shipment identifiers from 'shipmentIdentifiers': {identifiers}")
        else:
            # Log if shipmentIdentifiers is not found or not a list
            logging.warning(f"'shipmentIdentifiers' field not found or not a list in body. Body preview: {str(body_json)[:500]}")
    
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse body JSON for shipment identifiers: {e}")
        logging.error(f"Body string preview: {body_str[:200]}")
    except Exception as e:
        logging.error(f"Error extracting shipment identifiers: {e}")
    
    return identifiers

async def fetch_shipment_creation(master_shipment_id=None, shipment_identifier=None, instance="NA"):
    creds = get_credentials(instance)
    base_url = f"https://{creds['customer_id']}.{creds['domain']}/v1/"
    
    headers = {
        "Authorization": f"Bearer {creds['customer_id']} {creds['access_key']}",
        "Content-Type": "application/json",
        "Accept": "application/x-ndjson"
    }

    if not (master_shipment_id or shipment_identifier):
        raise ValueError("Either master_shipment_id OR shipment_identifier must be provided")

    total_start_time = time.time()
    logging.info("\n=== Querying Observe API for Shipment History ===")
    
    # Get current time and set time ranges
    now = datetime.datetime.now(timezone.utc)
    end_time = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    # Initialize response structure
    response_data = {
        "master_shipment_id": master_shipment_id,
        "shipment_identifier": shipment_identifier,
        "shipment_creation_url": None,
        "webhook_pushes_url": None,
        "push_tracking_updates_url": None,
        "extracted_sleuth_trace_id": None,
        "period_searched": None
    }

    # CHANGED: Search from oldest to newest (90 days ago → present)
    # First period: 90-60 days ago
    first_end_time = (now - datetime.timedelta(days=60)).replace(hour=23, minute=59, second=59, microsecond=999999)
    first_start_time = (now - datetime.timedelta(days=90)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    logging.info(f"\nFirst attempt - Querying data from {first_start_time.strftime('%Y-%m-%d %H:%M:%S')} to {first_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = await process_time_range(
        first_start_time, 
        first_end_time, 
        master_shipment_id, 
        shipment_identifier,
        instance
    )

    # Second period: 60-30 days ago
    if not results:
        second_end_time = (now - datetime.timedelta(days=30)).replace(hour=23, minute=59, second=59, microsecond=999999)
        second_start_time = (now - datetime.timedelta(days=60)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        logging.info(f"\nSecond attempt - Querying data from {second_start_time.strftime('%Y-%m-%d %H:%M:%S')} to {second_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        results = await process_time_range(
            second_start_time, 
            second_end_time, 
            master_shipment_id, 
            shipment_identifier,
            instance
        )

        # Third period: Last 30 days (most recent)
        if not results:
            third_end_time = end_time
            third_start_time = (now - datetime.timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
            
            logging.info(f"\nFinal attempt - Querying data from {third_start_time.strftime('%Y-%m-%d %H:%M:%S')} to {third_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            results = await process_time_range(
                third_start_time, 
                third_end_time, 
                master_shipment_id, 
                shipment_identifier,
                instance
            )

    # Process results if found in any period
    if results:
        logging.info("\nProcessing results...")
        # Get the first entry with sleuthTraceId (already sorted by pipeline, so this will be the oldest)
        oldest_result = next((result for result in results 
                            if result.get('sleuthTraceId')), None)
        
        if oldest_result:
            sleuth_trace_id = oldest_result.get('sleuthTraceId')
            timestamp = oldest_result.get('timestamp')
            
            # Store the extracted sleuthTraceId in response
            response_data["extracted_sleuth_trace_id"] = sleuth_trace_id
            
            # Extract masterShipmentId if not provided
            if not master_shipment_id:
                body_str = oldest_result.get('body', '')
                if body_str:
                    try:
                        body_json = json.loads(body_str)
                        if 'shipment' in body_json:
                            extracted_master_shipment_id = body_json['shipment'].get('masterShipmentId')
                            if extracted_master_shipment_id:
                                logging.info(f"\nExtracted masterShipmentId: {extracted_master_shipment_id}")
                                response_data["master_shipment_id"] = extracted_master_shipment_id
                    except json.JSONDecodeError:
                        logging.error("\nFailed to parse shipment body JSON")
            
            if timestamp:
                try:
                    # Convert nanoseconds to seconds and create datetime
                    timestamp_seconds = float(timestamp) / 1e9
                    url_start_time = datetime.datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
                    
                    # For shipment creation URL: use 1-day range from creation timestamp
                    url_end_time = url_start_time + datetime.timedelta(days=1)
                    
                    # Generate Observe URL
                    observe_url = (
                        f"https://{creds['customer_id']}.{creds['domain']}/workspace/{creds['instance_id']}/log-explorer?"
                        f"datasetId={creds['dataset_id']}&"
                        f"filter-sleuthTraceId={sleuth_trace_id}&"
                        f"time-start={url_start_time.strftime('%Y-%m-%dT%H.%M.%S')}%2B05.30&"
                        f"time-end={url_end_time.strftime('%Y-%m-%dT%H.%M.%S')}%2B05.30"  # Changed to use 1-day range
                    )
                    
                    response_data["shipment_creation_url"] = observe_url
                    response_data["period_searched"] = "found"
                    logging.info(f"\nGenerated Observe URL: {observe_url}")
                    
                    # NEW: Query for SERVER_REQUEST with the extracted sleuthTraceId
                    # Use 15 days before shipment creation to present time
                    # SERVER_REQUEST comes before SERVER_RESPONSE, so we search backwards
                    server_request_start_time = url_start_time - datetime.timedelta(days=15)
                    server_request_end_time = datetime.datetime.now(timezone.utc)
                    logging.info(f"\nQuerying SERVER_REQUEST logs for sleuthTraceId: {sleuth_trace_id}")
                    logging.info(f"Search range (15 days before to present): {server_request_start_time.strftime('%Y-%m-%d %H:%M:%S')} to {server_request_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    # Run SERVER_REQUEST and webhook pushes queries in parallel for better performance
                    server_request_task = asyncio.create_task(
                        get_server_request_by_sleuth_trace_id(
                            sleuth_trace_id,
                            server_request_start_time,
                            server_request_end_time,
                            instance
                        )
                    )
                    
                    webhook_task = None
                    if response_data["master_shipment_id"]:
                        logging.info("Starting webhook pushes query in parallel...")
                        webhook_task = asyncio.create_task(
                            get_webhook_pushes(
                                response_data["master_shipment_id"],
                                url_start_time,
                                end_time,
                                instance
                            )
                        )
                    
                    # Wait for SERVER_REQUEST query to complete
                    server_request_results = await server_request_task
                    
                    # Extract shipment identifiers from SERVER_REQUEST body
                    all_shipment_identifiers = set()
                    if server_request_results:
                        for sr_result in server_request_results:
                            body_str = sr_result.get('body', '')
                            if body_str:
                                identifiers = extract_shipment_identifiers(body_str)
                                all_shipment_identifiers.update(identifiers)
                        logging.info(f"Extracted {len(all_shipment_identifiers)} unique shipment identifiers")
                    else:
                        logging.warning("No SERVER_REQUEST results found for the sleuthTraceId")
                    
                    # Generate PUSH_TRACKING updates URL directly (no query needed!)
                    if all_shipment_identifiers:
                        logging.info(f"\nGenerating PUSH_TRACKING updates URL with {len(all_shipment_identifiers)} identifiers (no query - direct URL fabrication)")
                        
                        # Build the filter for body containing any of the shipment identifiers
                        body_filters = '&'.join([f"filter=body%7C~%7C{identifier}" for identifier in all_shipment_identifiers])
                        
                        # Log the filter query for PUSH_TRACKING
                        logging.info(f"PUSH_TRACKING filter - Shipment Identifiers: {list(all_shipment_identifiers)}")
                        
                        # Build the filter for requestURI (using = for exact match)
                        request_uri_filters = (
                            "filter=requestURI%7C%3D%7C%2Fapi%2Fcarriers%2Fv2%2Ftl%2Fshipments%2FstatusUpdates&"
                            "filter=requestURI%7C%3D%7C%2Fapi%2Fv4%2Fcapacityproviders%2Fltl%2Fshipments%2FstatusUpdates&"
                            "filter=requestURI%7C%3D%7C%2Fapi%2Fv4%2Fcapacityproviders%2Ftl%2Fshipments%2FstatusUpdates&"
                            "filter=requestURI%7C%3D%7C%2Fapi%2Fv4%2Fcapacityproviders%2Fparcel%2Fshipments%2FstatusUpdates"
                        )
                        
                        # Time range: from shipment creation to present (now)
                        push_tracking_end_time = datetime.datetime.now(timezone.utc)
                        
                        push_tracking_url = (
                            f"https://{creds['customer_id']}.{creds['domain']}/workspace/{creds['instance_id']}/log-explorer?"
                            f"datasetId={creds['dataset_id']}&"
                            f"{body_filters}&"
                            f"{request_uri_filters}&"
                            f"filter-type=SERVER_REQUEST&"
                            f"time-start={url_start_time.strftime('%Y-%m-%dT%H.%M.%S')}%2B05.30&"
                            f"time-end={push_tracking_end_time.strftime('%Y-%m-%dT%H.%M.%S')}%2B05.30"
                        )
                        
                        response_data["push_tracking_updates_url"] = push_tracking_url
                        logging.info(f"Generated PUSH_TRACKING updates URL: {push_tracking_url}")
                    else:
                        logging.info("\nNo shipment identifiers found, skipping PUSH_TRACKING updates URL generation")
                    
                    # Wait for webhook pushes query to complete
                    if webhook_task:
                        webhook_results = await webhook_task
                        
                        if webhook_results:
                            # Generate webhook pushes URL
                            logging.info(f"\nNumber of webhook results found: {len(webhook_results)}")
                            webhook_url = (
                                f"https://{creds['customer_id']}.{creds['domain']}/workspace/{creds['instance_id']}/log-explorer?"
                                f"datasetId={creds['dataset_id']}&"
                                f"filter=body%7C~%7C{response_data['master_shipment_id']}&"
                                f"filter-applicationName=push-processor&"
                                f"filter-type=CLIENT_REQUEST&"
                                f"time-start={url_start_time.strftime('%Y-%m-%dT%H.%M.%S')}%2B05.30&"
                                f"time-end={end_time.strftime('%Y-%m-%dT%H.%M.%S')}%2B05.30"
                            )
                            response_data["webhook_pushes_url"] = webhook_url
                            logging.info(f"Generated Webhook Pushes URL: {webhook_url}")
                        else:
                            logging.info("\nNo webhook push results found")
                            response_data["webhook_pushes_url"] = None
                
                except Exception as e:
                    logging.error(f"\nError generating URL: {e}")

    else:
        logging.info("\nNo results found in any of the 90-day periods")
        response_data["period_searched"] = "not_found"

    print_elapsed_time(total_start_time, "\nTotal execution time")
    
    # Log the final response data
    logging.info("\nFinal Response Data:")
    logging.info(json.dumps(response_data, indent=2))
    
    return response_data