import aiohttp
import asyncio
import json
import datetime
import time
from datetime import timezone

# Credentials
CUSTOMER_ID = "140271604703"
ACCESS_KEY = "-sco0vSc682wt7V2mdxk1Sg3sSaeamSj"

# Base URL format
base_url = f"https://{CUSTOMER_ID}.observeinc.com/v1/"

# Headers
headers = {
    "Authorization": f"Bearer {CUSTOMER_ID} {ACCESS_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/x-ndjson"
}

def print_elapsed_time(start_time, message):
    elapsed = time.time() - start_time
    print(f"{message}: {elapsed:.2f} seconds")

async def execute_query(start_time, end_time, page_size=10000, query_type="shipment", trace_id=None, pipeline=None, master_shipment_id=None, shipment_identifier=None, carrier_identifier=None):
    query_start_time = time.time()
    
    # Define query pipeline based on type
    if query_type == "shipment":
        if master_shipment_id:
            pipeline = (
                "filter type = 'SERVER_RESPONSE' and requestURI = '/api/v4/tl/shipments' | "
                f"filter contains(body, '{master_shipment_id}') | "
                "pick_col timestamp, sleuthTraceId, body | "
                "sort desc(timestamp)"
            )
        else:
            pipeline = (
                "filter type = 'SERVER_RESPONSE' and requestURI = '/api/v4/tl/shipments' | "
                f"filter contains(body, '{carrier_identifier}') and contains(body, '{shipment_identifier}') | "
                "pick_col timestamp, sleuthTraceId, body | "
                "sort desc(timestamp)"
            )
    elif query_type == "tracking" and trace_id:
        pipeline = (
            f"filter type = 'INTERNAL_SERVER_RESPONSE' and requestURI = '/vendors/tl/trackingmethods/query' | "
            f"filter sleuthTraceId = '{trace_id}' | "
            "pick_col timestamp, sleuthTraceId, body | "
            "sort desc(timestamp)"
        )
    elif query_type == "tracking_status" and pipeline:
        pass
    else:
        print(f"Unsupported query type: {query_type}")
        return []
    
    query_data = {
        "query": {
            "outputStage": "myStage",
            "stages": [
                {
                    "input": [
                        {
                            "inputName": "main",
                            "datasetId": "41231950"
                        }
                    ],
                    "stageID": "myStage",
                    "pipeline": pipeline
                }
            ]
        },
        "rowCount": str(page_size)
    }

    now = datetime.datetime.now(timezone.utc)
    adjusted_end_time = min(end_time, now)
    adjusted_start_time = min(start_time, adjusted_end_time)
    
    start_time_iso = adjusted_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time_iso = adjusted_end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    url = f"{base_url}meta/export/query?startTime={start_time_iso}&endTime={end_time_iso}&paginate=true"
    print(f"\nQuerying from {end_time_iso} to {start_time_iso}")
    
    try:
        async with aiohttp.ClientSession() as session:
            request_start = time.time()
            async with session.post(url, headers=headers, json=query_data, timeout=120) as response:
                print_elapsed_time(request_start, "Initial request took")
                
                if response.status == 400:
                    error_text = await response.text()
                    print(f"Error response: {error_text}")
                    return []
                
                if response.status == 202:
                    cursor_id = response.headers.get('X-Observe-Cursor-Id')
                    results = []
                    offset = 0
                    total_rows = None
                    polling_start = time.time()
                    last_progress_time = time.time()
                    
                    while True:
                        page_url = f"{base_url}meta/export/query/page?cursorId={cursor_id}&offset={offset}&numRows={page_size}"
                        
                        retry_count = 0
                        max_retries = 60
                        wait_time = 2
                        total_wait_time = 0
                        max_wait_time = 180
                        
                        while retry_count < max_retries and total_wait_time < max_wait_time:
                            try:
                                request_start = time.time()
                                async with session.get(page_url, headers=headers, timeout=60) as page_response:
                                    request_time = time.time() - request_start
                                    
                                    if page_response.status == 200:
                                        if total_rows is None:
                                            total_rows = int(page_response.headers.get('X-Observe-Total-Rows', '0'))
                                            print(f"Total rows expected: {total_rows}")
                                        
                                        batch_size = 2000
                                        lines = (await page_response.text()).splitlines()
                                        for i in range(0, len(lines), batch_size):
                                            batch = lines[i:i + batch_size]
                                            for line in batch:
                                                if line.strip():
                                                    try:
                                                        entry = json.loads(line)
                                                        if entry.get('sleuthTraceId'):
                                                            results.append(entry)
                                                    except json.JSONDecodeError:
                                                        continue
                                        
                                        offset += page_size
                                        if offset >= total_rows:
                                            print_elapsed_time(polling_start, "Total polling time")
                                            print_elapsed_time(query_start_time, "Total query execution time")
                                            return results
                                        
                                        print(f"Retrieved {len(results)} results so far")
                                        break
                                    
                                    elif page_response.status == 202:
                                        current_time = time.time()
                                        if current_time - last_progress_time >= 30:
                                            print(f"\nStill processing... (Wait: {total_wait_time}s)")
                                            last_progress_time = current_time
                                        
                                        if total_wait_time < 30:
                                            wait_time = 2
                                        elif total_wait_time < 60:
                                            wait_time = 5
                                        else:
                                            wait_time = 10
                                        
                                        await asyncio.sleep(wait_time)
                                        total_wait_time += wait_time
                                        retry_count += 1
                                    
                                    elif page_response.status in [500, 503]:
                                        print(f"\nServer error {page_response.status}, retrying...")
                                        await asyncio.sleep(5)
                                        continue
                                    elif page_response.status == 410:
                                        print("\nCursor expired, retrying query...")
                                        return await execute_query(adjusted_start_time, adjusted_end_time, page_size)
                                    else:
                                        print(f"\nError response: {page_response.status}")
                                        return results
                            
                            except aiohttp.ClientError as e:
                                print(f"\nRequest error: {str(e)}")
                                await asyncio.sleep(5)
                                total_wait_time += 5
                                retry_count += 1
                        
                        if total_wait_time >= max_wait_time:
                            print(f"\nMaximum wait time reached")
                            break
                
                return results
    
    except aiohttp.ClientError as e:
        print(f"Initial request error: {e}")
        return []

async def get_tracking_status_data(start_time, end_time, vendor_type, master_shipment_id, shipment_identifier=None):
    tracking_status_start_time = time.time()
    
    # Use 1-day chunks for tracking status
    chunk_delta = datetime.timedelta(days=1)
    chunks = []
    
    # Create chunks from newest to oldest
    chunk_start = start_time
    while chunk_start < end_time:
        chunk_end = min(chunk_start + chunk_delta, end_time)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end
    
    print(f"\nWill process tracking status in {len(chunks)} chunks")
    
    # Construct pipeline based on vendor type
    if vendor_type == "API_PUSH":
        vendor_pipeline = (
            f"filter requestURI = '/vendors/PUSH_TRACKING/1/tl/shipments/statuses/query' and "
            f"body ~ '{master_shipment_id}' | "
            "pick_col timestamp, sleuthTraceId, body | "
            "sort desc(timestamp)"
        )
        carrier_pipeline = (
            f"filter requestURI = '/api/v4/capacityproviders/tl/shipments/statusUpdates' and "
            f"body ~ '{shipment_identifier}' | "
            "pick_col timestamp, sleuthTraceId, body | "
            "sort desc(timestamp)"
        )
    elif vendor_type == "MOBILE_PHONE":
        pipeline = (
            f"filter requestURI = '/vendors/MOBILEAPP/1/tl/shipments/statuses/query' and "
            f"body ~ '{master_shipment_id}' | "
            "pick_col timestamp, sleuthTraceId, body | "
            "sort desc(timestamp)"
        )
    else:
        print(f"Unsupported vendor type: {vendor_type}")
        return [] if vendor_type == "MOBILE_PHONE" else ([], [])
    
    print("\nTracking Status Query Pipeline:")
    print(vendor_pipeline if vendor_type == "API_PUSH" else pipeline)
    
    # For API_PUSH, process vendor and carrier updates in parallel
    if vendor_type == "API_PUSH" and shipment_identifier:
        # Create tasks for vendor and carrier updates
        vendor_tasks = [
            execute_query(chunk_start, chunk_end, 10000, "tracking_status", pipeline=vendor_pipeline)
            for chunk_start, chunk_end in chunks
        ]
        carrier_tasks = [
            execute_query(chunk_start, chunk_end, 10000, "tracking_status", pipeline=carrier_pipeline)
            for chunk_start, chunk_end in chunks
        ]
        
        # Combine vendor and carrier tasks into a single list
        all_tasks = vendor_tasks + carrier_tasks

        # Run all tasks in parallel
        results = await asyncio.gather(*all_tasks)

        # Separate vendor and carrier results
        vendor_results = results[:len(vendor_tasks)]
        carrier_results = results[len(vendor_tasks):]
    else:  # For MOBILE_PHONE, only do vendor tracking
        tasks = [
            execute_query(chunk_start, chunk_end, 10000, "tracking_status", pipeline=pipeline)
            for chunk_start, chunk_end in chunks
        ]
        vendor_results = await asyncio.gather(*tasks)
        carrier_results = []
    
    print_elapsed_time(tracking_status_start_time, "\nTotal tracking status query time")
    
    # Deduplicate vendor and carrier results
    seen_vendor_bodies = set()
    unique_vendor_results = []
    for result in vendor_results:
        if isinstance(result, list):
            for entry in result:
                body_str = entry.get('body', '') if isinstance(entry, dict) else ''
                if body_str and body_str not in seen_vendor_bodies:
                    seen_vendor_bodies.add(body_str)
                    unique_vendor_results.append(entry)
        elif isinstance(result, dict):
            body_str = result.get('body', '')
            if body_str and body_str not in seen_vendor_bodies:
                seen_vendor_bodies.add(body_str)
                unique_vendor_results.append(result)
    
    seen_carrier_bodies = set()
    unique_carrier_results = []
    for result in carrier_results:
        if isinstance(result, list):
            for entry in result:
                body_str = entry.get('body', '') if isinstance(entry, dict) else ''
                if body_str and body_str not in seen_carrier_bodies:
                    seen_carrier_bodies.add(body_str)
                    unique_carrier_results.append(entry)
        elif isinstance(result, dict):
            body_str = result.get('body', '')
            if body_str and body_str not in seen_carrier_bodies:
                seen_carrier_bodies.add(body_str)
                unique_carrier_results.append(result)
    
    print(f"\nFound {len(unique_vendor_results)} unique vendor tracking entries")
    print(f"Found {len(unique_carrier_results)} unique carrier push entries")
    return unique_vendor_results, unique_carrier_results

async def fetch_all_data(master_shipment_id=None, shipment_identifier=None, carrier_identifier=None):
    if not (master_shipment_id or (shipment_identifier and carrier_identifier)):
        raise ValueError("Either master_shipment_id OR both shipment_identifier and carrier_identifier must be provided")

    total_start_time = time.time()
    print("\n=== Querying Observe API ===")
    
    end_time = datetime.datetime.now(timezone.utc)
    start_time = end_time - datetime.timedelta(days=30)
    
    print("\nFetching shipment data...")
    shipment_results = []
    
    chunk_delta = datetime.timedelta(days=1)
    chunks = []
    
    chunk_start = start_time
    while chunk_start < end_time:
        chunk_end = min(chunk_start + chunk_delta, end_time)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end
    
    print(f"Will process all {len(chunks)} chunks in parallel")
    
    # Process all chunks in parallel for shipment data
    tasks = [
        execute_query(start_time, end_time, 10000, "shipment", master_shipment_id=master_shipment_id, shipment_identifier=shipment_identifier, carrier_identifier=carrier_identifier)
        for start_time, end_time in chunks
    ]
    results = await asyncio.gather(*tasks)
    
    for chunk_num, result in enumerate(results):
        if result:
            print(f"\nChunk {chunk_num}: Found {len(result)} results")
            shipment_results.extend(result)
        else:
            print(f"\nChunk {chunk_num}: No results found")
    
    # Process shipment results and return tracking data
    tracking_data_response = {}
    
    if shipment_results:
        print("\nProcessing shipment results...")
        seen = set()
        unique_results = []
        for result in shipment_results:
            trace_id = result.get('sleuthTraceId')
            if trace_id and trace_id not in seen:
                seen.add(trace_id)
                unique_results.append(result)

        # For each unique shipment, get tracking method
        print("\nFetching tracking methods...")
        for entry in unique_results:
            trace_id = entry['sleuthTraceId']
            print(f"\nFetching tracking method for Trace ID: {trace_id}")
            
            # Query for tracking method
            tracking_results = []
            try:
                tracking_results = await execute_query(start_time, end_time, 10000, "tracking", trace_id)
            except Exception as e:
                print(f"\nError fetching tracking method: {str(e)}")
                continue
            
            # Process tracking results
            vendor_type = None
            if tracking_results:
                print(f"\nFound {len(tracking_results)} tracking results")
                for tracking_entry in tracking_results:
                    body_str = tracking_entry.get('body', '')
                    if body_str:
                        try:
                            tracking_data = json.loads(body_str)
                            if isinstance(tracking_data, list) and len(tracking_data) > 0:
                                vendor_type = tracking_data[0].get('vendorType')
                                break
                        except json.JSONDecodeError:
                            continue
            
            # Extract or use provided masterShipmentId and shipment identifier
            if master_shipment_id:
                current_master_shipment_id = master_shipment_id
                current_shipment_identifier = None
                body_str = entry.get('body', '')
                if body_str:
                    try:
                        body_json = json.loads(body_str)
                        if 'shipment' in body_json:
                            shipment_identifiers = body_json['shipment'].get('shipmentIdentifiers', [])
                            if shipment_identifiers:
                                current_shipment_identifier = shipment_identifiers[0].get('value')
                    except json.JSONDecodeError:
                        pass
            else:
                current_master_shipment_id = None
                current_shipment_identifier = shipment_identifier
                body_str = entry.get('body', '')
                if body_str:
                    try:
                        body_json = json.loads(body_str)
                        if 'shipment' in body_json:
                            current_master_shipment_id = body_json['shipment'].get('masterShipmentId')
                    except json.JSONDecodeError:
                        pass

            # Get tracking status data if we have both vendor type and master shipment id
            if vendor_type and current_master_shipment_id:
                tracking_data_response["tracking_data"] = {}
                
                if vendor_type == "API_PUSH":
                    if current_shipment_identifier:
                        vendor_results, carrier_results = await get_tracking_status_data(
                            start_time, end_time, vendor_type, current_master_shipment_id, 
                            current_shipment_identifier
                        )
                        # Process and store vendor updates
                        vendor_updates = []
                        for status in vendor_results:
                            status_body = status.get('body', '')
                            if status_body:
                                try:
                                    parsed_body = json.loads(status_body)
                                    vendor_updates.append(parsed_body)
                                except json.JSONDecodeError:
                                    continue
                        
                        # Process and store carrier updates
                        carrier_updates = []
                        for status in carrier_results:
                            status_body = status.get('body', '')
                            if status_body:
                                try:
                                    parsed_body = json.loads(status_body)
                                    carrier_updates.append(parsed_body)
                                except json.JSONDecodeError:
                                    continue
                        
                        tracking_data_response["tracking_data"]["vendor_updates"] = vendor_updates
                        tracking_data_response["tracking_data"]["carrier_updates"] = carrier_updates
                    
                elif vendor_type == "MOBILE_PHONE":
                    vendor_results = await get_tracking_status_data(
                        start_time, end_time, vendor_type, current_master_shipment_id
                    )
                    
                    # Process and store vendor updates
                    vendor_updates = []
                    # Check if vendor_results is a tuple or list
                    if isinstance(vendor_results, tuple):
                        results_to_process = vendor_results[0]  # If tuple, take first element
                    else:
                        results_to_process = vendor_results  # If list, use as is
                    
                    for status in results_to_process:
                        status_body = status.get('body', '')
                        if status_body:
                            try:
                                parsed_body = json.loads(status_body)
                                vendor_updates.append(parsed_body)
                            except json.JSONDecodeError:
                                continue
                    
                    tracking_data_response["tracking_data"] = {
                        "vendor_updates": vendor_updates,
                        "carrier_updates": []  # Always empty for MOBILE_PHONE
                    }
                
                else:
                    print(f"\nUnsupported vendor type: {vendor_type}")
                    tracking_data_response["tracking_data"] = {"vendor_updates": []}

    print_elapsed_time(total_start_time, "\nTotal execution time")
    # Convert the tracking_data_response to a stringified JSON before returning
    return json.dumps(tracking_data_response)

if __name__ == "__main__":
    try:
        # Use asyncio.run to execute the async function
                # Example usage:
        # Case 1: Using master shipment id
        # fetch_all_data(master_shipment_id="6fc36462-15f1-45b4-921b-2816bc31b872")
        # fetch_all_data(master_shipment_id="68b82886-2ff5-42df-9bfc-c4cf2f65fc7f")
        
        # Case 2: Using shipment identifier and carrier identifier
        # fetch_all_data(shipment_identifier="your-bol-number", carrier_identifier="your-scac-code")
        
        # For testing, using hardcoded values
        # fetch_all_data(shipment_identifier="404002547466", carrier_identifier="143155")
        # fetch_all_data(shipment_identifier="312957", carrier_identifier="3197835")
        output = asyncio.run(fetch_all_data(master_shipment_id="6fc36462-15f1-45b4-921b-2816bc31b872"))
        print("\nFunction Return Value:")
        print(output)
    except Exception as e:
        print(f"Error: {str(e)}")