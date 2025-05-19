import requests
import json
import datetime
import time
from datetime import timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def execute_query(start_time, end_time, page_size=10000, query_type="shipment", trace_id=None, pipeline=None, master_shipment_id=None, shipment_identifier=None, carrier_identifier=None):
    query_start_time = time.time()
    
    # Define query pipeline based on type
    if query_type == "shipment":
        if master_shipment_id:
            # Case 1: Use master shipment ID for filtering
            pipeline = (
                "filter type = 'SERVER_RESPONSE' and requestURI = '/api/v4/tl/shipments' | "
                f"filter contains(body, '{master_shipment_id}') | "
                "pick_col timestamp, sleuthTraceId, body | "
                "sort desc(timestamp)"
            )
        else:
            # Case 2: Use shipment identifier and carrier identifier
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
        # Pipeline is already provided, no need for message
        pass
    else:
        print(f"Unsupported query type: {query_type}")
        return []
    
    # Optimize the query pipeline to reduce data processing
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

    # Ensure we're not querying future dates
    now = datetime.datetime.now(timezone.utc)
    adjusted_end_time = min(end_time, now)
    adjusted_start_time = min(start_time, adjusted_end_time)
    
    start_time_iso = adjusted_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time_iso = adjusted_end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    url = f"{base_url}meta/export/query?startTime={start_time_iso}&endTime={end_time_iso}&paginate=true"
    print(f"\nQuerying from {end_time_iso} to {start_time_iso}")
    
    try:
        request_start = time.time()
        response = requests.post(url, headers=headers, json=query_data, timeout=120)
        print_elapsed_time(request_start, "Initial request took")
        
        if response.status_code == 400:
            print(f"Error response: {response.text}")
            return []
            
        if response.status_code == 202:
            cursor_id = response.headers.get('X-Observe-Cursor-Id')
            results = []
            offset = 0
            total_rows = None
            polling_start = time.time()
            last_progress_time = time.time()
            
            while True:
                page_url = f"{base_url}meta/export/query/page?cursorId={cursor_id}&offset={offset}&numRows={page_size}"
                
                retry_count = 0
                max_retries = 60  # Reduced from 120
                wait_time = 2  # Start with shorter wait times
                total_wait_time = 0
                max_wait_time = 180  # Reduced from 300
                
                while retry_count < max_retries and total_wait_time < max_wait_time:
                    try:
                        request_start = time.time()
                        page_response = requests.get(page_url, headers=headers, timeout=60)
                        request_time = time.time() - request_start
                        
                        if page_response.status_code == 200:
                            if total_rows is None:
                                total_rows = int(page_response.headers.get('X-Observe-Total-Rows', '0'))
                                print(f"Total rows expected: {total_rows}")
                            
                            # Process results in batches
                            batch_size = 2000
                            lines = page_response.text.splitlines()
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
                            
                        elif page_response.status_code == 202:
                            current_time = time.time()
                            if current_time - last_progress_time >= 30:
                                print(f"\nStill processing... (Wait: {total_wait_time}s)")
                                last_progress_time = current_time
                            
                            # Adaptive waiting: Start with shorter intervals, increase if taking longer
                            if total_wait_time < 30:
                                wait_time = 2
                            elif total_wait_time < 60:
                                wait_time = 5
                            else:
                                wait_time = 10
                                
                            time.sleep(wait_time)
                            total_wait_time += wait_time
                            retry_count += 1
                            
                        elif page_response.status_code in [500, 503]:
                            print(f"\nServer error {page_response.status_code}, retrying...")
                            time.sleep(5)
                            continue
                        elif page_response.status_code == 410:
                            print("\nCursor expired, retrying query...")
                            return execute_query(adjusted_start_time, adjusted_end_time, page_size)
                        else:
                            print(f"\nError response: {page_response.status_code}")
                            return results
                            
                    except requests.exceptions.RequestException as e:
                        print(f"\nRequest error: {str(e)}")
                        time.sleep(5)
                        total_wait_time += 5
                        retry_count += 1
                
                if total_wait_time >= max_wait_time:
                    print(f"\nMaximum wait time reached")
                    break
                
        return results
        
    except requests.exceptions.RequestException as e:
        print(f"Initial request error: {e}")
        return []

def process_chunk(chunk_info):
    chunk_num, total_chunks, start_time, end_time = chunk_info
    print(f"\nProcessing chunk {chunk_num}/{total_chunks}")
    print(f"Time range: {end_time.strftime('%Y-%m-%d %H:%M:%S')} to {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    chunk_start_time = time.time()
    max_retries = 2
    
    for retry in range(max_retries):
        try:
            results = execute_query(start_time, end_time)
            print_elapsed_time(chunk_start_time, f"Chunk {chunk_num} processing time")
            return results or []
        except Exception as e:
            if retry < max_retries - 1:
                print(f"Retrying chunk {chunk_num} after error: {str(e)}")
                time.sleep(2)
            else:
                print(f"Failed to process chunk {chunk_num} after {max_retries} attempts")
                return []

def get_tracking_method(body_str):
    print("\nTracking Method Response Body:")
    print(body_str)
    try:
        tracking_data = json.loads(body_str)
        if isinstance(tracking_data, list) and len(tracking_data) > 0:
            vendor_type = tracking_data[0].get('vendorType')
            print(f"\nVendor Type found: {vendor_type}")
            if vendor_type == "API_PUSH":
                return "Push Tracking"
            elif vendor_type == "MOBILE_PHONE":
                return "Mobile Tracking"
            else:
                return f"Other ({vendor_type})"
        print("\nNo vendor type found in tracking data")
        return "Unknown"
    except json.JSONDecodeError:
        print("\nError: Could not parse tracking response body as JSON")
        return "Unknown"
    except (KeyError, IndexError) as e:
        print(f"\nError: Could not extract vendor type from tracking data: {str(e)}")
        return "Unknown"

def get_carrier_push_data(start_time, end_time, shipment_identifier):
    tracking_status_start_time = time.time()
    all_results = []
    
    # Use 1-day chunks for tracking status
    chunk_delta = datetime.timedelta(days=1)
    chunks = []
    
    # Create chunks from newest to oldest
    chunk_start = start_time
    while chunk_start < end_time:
        chunk_end = min(chunk_start + chunk_delta, end_time)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end
    
    print(f"\nProcessing carrier push data in {len(chunks)} chunks")
    
    # Construct pipeline for carrier push using shipment identifier
    pipeline = (
        f"filter requestURI = '/api/v4/capacityproviders/tl/shipments/statusUpdates' and "
        f"body ~ '{shipment_identifier}' | "
        "pick_col timestamp, sleuthTraceId, body | "
        "sort desc(timestamp)"
    )
    
    # Process chunks in parallel
    with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
        futures = []
        for i, (chunk_start, chunk_end) in enumerate(reversed(chunks), 1):
            future = executor.submit(execute_query, chunk_start, chunk_end, 10000, "tracking_status", pipeline=pipeline)
            futures.append((i, future))
        
        # Collect results from all chunks
        for chunk_num, future in futures:
            try:
                chunk_results = future.result()
                if chunk_results:
                    all_results.extend(chunk_results)
            except Exception as e:
                print(f"\nError in carrier push chunk {chunk_num}: {str(e)}")
    
    print_elapsed_time(tracking_status_start_time, "\nCarrier push query time")
    
    # Remove duplicates based on body content
    seen_bodies = set()
    unique_results = []
    for result in all_results:
        body_str = result.get('body', '')
        if body_str and body_str not in seen_bodies:
            seen_bodies.add(body_str)
            unique_results.append(result)
    
    return unique_results

def get_tracking_status_data(start_time, end_time, vendor_type, master_shipment_id, shipment_identifier=None):
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
        pipeline = (
            f"filter requestURI = '/vendors/PUSH_TRACKING/1/tl/shipments/statuses/query' and "
            f"body ~ '{master_shipment_id}' | "
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
    print(pipeline)
    
    vendor_results = []
    carrier_results = []
    
    # For API_PUSH, create two thread pools - one for vendor tracking and one for carrier push
    if vendor_type == "API_PUSH" and shipment_identifier:
        with ThreadPoolExecutor(max_workers=len(chunks)) as vendor_executor, \
             ThreadPoolExecutor(max_workers=len(chunks)) as carrier_executor:
            
            # Submit vendor tracking queries
            vendor_futures = []
            for i, (chunk_start, chunk_end) in enumerate(reversed(chunks), 1):
                future = vendor_executor.submit(execute_query, chunk_start, chunk_end, 10000, "tracking_status", pipeline=pipeline)
                vendor_futures.append((i, future))
            
            # Submit carrier push queries in parallel using shipment identifier
            carrier_futures = []
            for i, (chunk_start, chunk_end) in enumerate(reversed(chunks), 1):
                future = carrier_executor.submit(execute_query, chunk_start, chunk_end, 10000, "tracking_status", 
                    pipeline=(f"filter requestURI = '/api/v4/capacityproviders/tl/shipments/statusUpdates' and "
                             f"body ~ '{shipment_identifier}' | "
                             "pick_col timestamp, sleuthTraceId, body | "
                             "sort desc(timestamp)"))
                carrier_futures.append((i, future))
            
            # Collect vendor tracking results
            for chunk_num, future in vendor_futures:
                try:
                    chunk_results = future.result()
                    if chunk_results:
                        vendor_results.extend(chunk_results)
                except Exception as e:
                    print(f"\nError in vendor tracking chunk {chunk_num}: {str(e)}")
            
            # Collect carrier push results
            for chunk_num, future in carrier_futures:
                try:
                    chunk_results = future.result()
                    if chunk_results:
                        carrier_results.extend(chunk_results)
                except Exception as e:
                    print(f"\nError in carrier push chunk {chunk_num}: {str(e)}")
    else:  # For MOBILE_PHONE, only do vendor tracking
        with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
            futures = []
            for i, (chunk_start, chunk_end) in enumerate(reversed(chunks), 1):
                future = executor.submit(execute_query, chunk_start, chunk_end, 10000, "tracking_status", pipeline=pipeline)
                futures.append((i, future))
            
            for chunk_num, future in futures:
                try:
                    chunk_results = future.result()
                    if chunk_results:
                        vendor_results.extend(chunk_results)
                except Exception as e:
                    print(f"\nError in vendor tracking chunk {chunk_num}: {str(e)}")
    
    print_elapsed_time(tracking_status_start_time, "\nTotal tracking status query time")
    
    # Deduplicate vendor results
    seen_vendor_bodies = set()
    unique_vendor_results = []
    for result in vendor_results:
        body_str = result.get('body', '')
        if body_str and body_str not in seen_vendor_bodies:
            seen_vendor_bodies.add(body_str)
            unique_vendor_results.append(result)
    
    # For API_PUSH, deduplicate carrier results
    if vendor_type == "API_PUSH" and shipment_identifier:
        seen_carrier_bodies = set()
        unique_carrier_results = []
        for result in carrier_results:
            body_str = result.get('body', '')
            if body_str and body_str not in seen_carrier_bodies:
                seen_carrier_bodies.add(body_str)
                unique_carrier_results.append(result)
        
        print(f"\nFound {len(unique_vendor_results)} unique vendor tracking entries")
        print(f"Found {len(unique_carrier_results)} unique carrier push entries")
        return unique_vendor_results, unique_carrier_results
    else:
        print(f"\nFound {len(unique_vendor_results)} unique vendor tracking entries")
        return unique_vendor_results

def fetch_all_data(master_shipment_id=None, shipment_identifier=None, carrier_identifier=None):
    if not (master_shipment_id or (shipment_identifier and carrier_identifier)):
        raise ValueError("Either master_shipment_id OR both shipment_identifier and carrier_identifier must be provided")

    total_start_time = time.time()
    print("\n=== Querying Observe API ===")
    
    end_time = datetime.datetime.now(timezone.utc)
    start_time = end_time - datetime.timedelta(days=30)
    
    # First get shipment data
    print("\nFetching shipment data...")
    shipment_results = []
    
    # Use 3 day chunks
    chunk_delta = datetime.timedelta(days=1)
    chunks = []
    
    # Create chunks from newest to oldest
    chunk_start = start_time
    while chunk_start < end_time:
        chunk_end = min(chunk_start + chunk_delta, end_time)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end
    
    print(f"Will process all {len(chunks)} chunks in parallel")
    
    # Process all chunks in parallel for shipment data
    with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
        futures = []
        for i, (chunk_start, chunk_end) in enumerate(reversed(chunks), 1):
            print(f"Submitting chunk {i}/{len(chunks)}")
            future = executor.submit(
                execute_query, 
                chunk_start, 
                chunk_end, 
                10000, 
                "shipment", 
                master_shipment_id=master_shipment_id,
                shipment_identifier=shipment_identifier,
                carrier_identifier=carrier_identifier
            )
            futures.append((i, future))
        
        for chunk_num, future in futures:
            try:
                chunk_results = future.result()
                if chunk_results:
                    print(f"\nChunk {chunk_num}: Found {len(chunk_results)} results")
                    shipment_results.extend(chunk_results)
                else:
                    print(f"\nChunk {chunk_num}: No results found")
            except Exception as e:
                print(f"\nError in chunk {chunk_num}: {str(e)}")
    
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
                tracking_results = execute_query(start_time, end_time, 10000, "tracking", trace_id)
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
                        vendor_results, carrier_results = get_tracking_status_data(
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
                    vendor_results = get_tracking_status_data(
                        start_time, end_time, vendor_type, current_master_shipment_id
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
                    
                    tracking_data_response["tracking_data"] = {"vendor_updates": vendor_updates}
                
                else:
                    print(f"\nUnsupported vendor type: {vendor_type}")
                    tracking_data_response["tracking_data"] = {"vendor_updates": []}

    print_elapsed_time(total_start_time, "\nTotal execution time")
    # Convert the tracking_data_response to a stringified JSON before returning
    return json.dumps(tracking_data_response)

if __name__ == "__main__":
    try:
        # Example usage:
        # Case 1: Using master shipment id
        # fetch_all_data(master_shipment_id="2b81ef2d-0762-420e-b1aa-85cecea86d55")
        # fetch_all_data(master_shipment_id="68b82886-2ff5-42df-9bfc-c4cf2f65fc7f")
        
        # Case 2: Using shipment identifier and carrier identifier
        # fetch_all_data(shipment_identifier="your-bol-number", carrier_identifier="your-scac-code")
        
        # For testing, using hardcoded values
        # fetch_all_data(shipment_identifier="404002547466", carrier_identifier="143155")
        # fetch_all_data(shipment_identifier="312957", carrier_identifier="3197835")
        output = fetch_all_data(master_shipment_id="68b82886-2ff5-42df-9bfc-c4cf2f65fc7f")
        print("\nFunction Return Value:")
        print(output)
    except Exception as e:
        print(f"Error: {str(e)}")