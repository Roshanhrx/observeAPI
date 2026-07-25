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

async def execute_query(start_time, end_time, page_size=10000, query_type="shipment", trace_id=None, pipeline=None, master_shipment_id=None, shipment_identifier=None, carrier_identifier=None, instance="NA"):
    query_start_time = time.time()
    results = []  # Initialize results here
    
    # Get credentials based on instance
    creds = get_credentials(instance)
    base_url = f"https://{creds['customer_id']}.{creds['domain']}/v1/"
    
    headers = {
        "Authorization": f"Bearer {creds['customer_id']} {creds['access_key']}",
        "Content-Type": "application/json",
        "Accept": "application/x-ndjson"
    }

    # Define query pipeline based on type
    if query_type == "shipment":
        if master_shipment_id:
            pipeline = (
                "filter type = 'SERVER_RESPONSE' and requestURI = '/api/v4/tl/shipments' | "
                f"filter body ~ '{master_shipment_id}' | "
                "pick_col timestamp, sleuthTraceId, body | "
                "sort desc(timestamp)"
            )
        else:
            pipeline = (
                "filter type = 'SERVER_RESPONSE' and requestURI = '/api/v4/tl/shipments' | "
                f"filter body ~ '{carrier_identifier}' and body ~ '{shipment_identifier}' | "
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
        logging.info(f"Unsupported query type: {query_type}")
        return results  # Return empty results list
    
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
    
    # Note: We're keeping startTime as the earlier date and endTime as the later date
    url = f"{base_url}meta/export/query?startTime={start_time_iso}&endTime={end_time_iso}&paginate=true"
    
    try:
        async with aiohttp.ClientSession() as session:
            request_start = time.time()
            async with session.post(url, headers=headers, json=query_data, timeout=240) as response:
                # Add 401 error handling
                if response.status == 401:
                    error_text = await response.text()
                    logging.info(f"\nAuthentication failed. Error: {error_text}")
                    raise Exception("Authentication failed with Observe API")
                
                if response.status == 400:
                    error_text = await response.text()
                    logging.info(f"Bad request error: {error_text}")
                    return results 
                
                if response.status == 202:
                    cursor_id = response.headers.get('X-Observe-Cursor-Id')
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
                                async with session.get(page_url, headers=headers, timeout=240) as page_response:
                                    request_time = time.time() - request_start
                                    
                                    if page_response.status == 200:
                                        if total_rows is None:
                                            total_rows = int(page_response.headers.get('X-Observe-Total-Rows', '0'))
                                        
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
                                            
                                            return results
                                        
                                        break
                                    
                                    elif page_response.status == 202:
                                        current_time = time.time()
                                        if current_time - last_progress_time >= 30:
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
                                        logging.info(f"\nServer error {page_response.status}, retrying...")
                                        await asyncio.sleep(5)
                                        continue
                                    elif page_response.status == 410:
                                        logging.info("\nCursor expired, retrying query...")
                                        return await execute_query(start_time, end_time, page_size, instance=instance)
                                    else:
                                        logging.info(f"\nError response: {page_response.status}")
                                        return results
                            
                            except aiohttp.ClientError as e:
                                logging.info(f"\nRequest error: {str(e)}")
                                await asyncio.sleep(5)
                                total_wait_time += 5
                                retry_count += 1
                        
                        if total_wait_time >= max_wait_time:
                            logging.info(f"\nMaximum wait time reached")
                            break
                
                return results
    
    except aiohttp.ClientError as e:
        logging.info(f"Initial request error: {e}")
        return results
        raise Exception("API request failed", str(e))

async def get_tracking_status_data(start_time, end_time, vendor_type, master_shipment_id, shipment_identifier=None, instance="NA"):
    tracking_status_start_time = time.time()
    
    # Get credentials based on instance
    creds = get_credentials(instance)
    
    # Use 1-day chunks for tracking status
    chunk_delta = datetime.timedelta(hours=24)
    chunks = []
    
    # Create chunks from newest to oldest
    chunk_start = start_time
    while chunk_start < end_time:
        chunk_end = min(chunk_start + chunk_delta, end_time)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end
    
    logging.info(f"\nWill process tracking status in {len(chunks)} chunks")
    
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
        logging.info(f"Unsupported vendor type: {vendor_type}")
        return [] if vendor_type == "MOBILE_PHONE" else ([], [])
    
    logging.info("\nTracking Status Query Pipeline:")
    logging.info(vendor_pipeline if vendor_type == "API_PUSH" else pipeline)
    
    # For API_PUSH, process vendor and carrier updates in parallel
    if vendor_type == "API_PUSH" and shipment_identifier:
        # Create tasks for vendor and carrier updates
        vendor_tasks = [
            execute_query(chunk_start, chunk_end, 10000, "tracking_status", pipeline=vendor_pipeline, instance=instance)
            for chunk_start, chunk_end in chunks
        ]
        carrier_tasks = [
            execute_query(chunk_start, chunk_end, 10000, "tracking_status", pipeline=carrier_pipeline, instance=instance)
            for chunk_start, chunk_end in chunks
        ]
        
        # Combine vendor and carrier tasks into a single list
        all_tasks = vendor_tasks + carrier_tasks

        # Run all tasks in parallel
        try:
            results = await asyncio.gather(*all_tasks)
        except Exception as e:
            logging.info(f"Error processing tracking status: {e}")
            raise Exception("Error processing tracking status", str(e))

        # Separate vendor and carrier results
        vendor_results = results[:len(vendor_tasks)]
        carrier_results = results[len(vendor_tasks):]
    else:  # For MOBILE_PHONE, only do vendor tracking
        tasks = [
            execute_query(chunk_start, chunk_end, 10000, "tracking_status", pipeline=pipeline, instance=instance)
            for chunk_start, chunk_end in chunks
        ]
        try:
            vendor_results = await asyncio.gather(*tasks)
        except Exception as e:
            logging.info(f"Error processing tracking status: {e}")
            raise Exception("Error processing tracking status", str(e))
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
    
    logging.info(f"\nFound {len(unique_vendor_results)} unique vendor tracking entries")
    logging.info(f"Found {len(unique_carrier_results)} unique carrier push entries")
    return unique_vendor_results, unique_carrier_results

async def fetch_all_data(master_shipment_id=None, shipment_identifier=None, carrier_identifier=None, instance="NA"):
    if not (master_shipment_id or (shipment_identifier and carrier_identifier)):
        raise ValueError("Either master_shipment_id OR both shipment_identifier and carrier_identifier must be provided")

    # Get credentials based on instance
    creds = get_credentials(instance)

    total_start_time = time.time()
    logging.info("\n=== Querying Observe API ===")
    
    # Get current time and set it to end of day
    now = datetime.datetime.now(timezone.utc)
    end_time = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    # Calculate start time as 30 days before current date, starting at 00:00:00
    start_time = (now - datetime.timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    logging.info(f"\nQuerying data from {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    logging.info("\nFetching shipment data...")
    shipment_results = []
    
    chunk_delta = datetime.timedelta(hours=24)
    chunks = []
    
    current_chunk_start = start_time
    while current_chunk_start < end_time:
        current_chunk_end = min(current_chunk_start + chunk_delta, end_time)
        chunks.append((current_chunk_start, current_chunk_end))
        current_chunk_start = current_chunk_end
    
    logging.info(f"Will process all {len(chunks)} chunks in parallel")
    
    # Process all chunks in parallel for shipment data
    tasks = [
        execute_query(chunk_start, chunk_end, 10000, "shipment", 
                     master_shipment_id=master_shipment_id, 
                     shipment_identifier=shipment_identifier, 
                     carrier_identifier=carrier_identifier,
                     instance=instance)
        for chunk_start, chunk_end in chunks
    ]
    results = await asyncio.gather(*tasks)
    
    for chunk_num, result in enumerate(results):
        if result:
            shipment_results.extend(result)
    
    # Initialize response structure early with default values
    final_response = {
        "shipment_created_timestamp": None,
        "observe_urls": {},
        "tracking_data": {}  # Changed from nested data structure
    }

    # After getting shipment results, if none found, return early
    if not shipment_results:
        logging.info("\nNo shipment results found")
        return json.dumps(final_response)

    # Process shipment results and return tracking data
    shipment_created_timestamp = None  # Initialize the timestamp variable
    
    if shipment_results:
        logging.info("\nProcessing shipment results...")
        # Get the first result with a valid trace_id
        first_valid_result = next((result for result in shipment_results if result.get('sleuthTraceId')), None)
        
        if first_valid_result:
            # Store the timestamp from the first valid result
            shipment_created_timestamp = first_valid_result.get('timestamp')
            
            # Convert nanoseconds to ISO format
            try:
                timestamp_seconds = int(shipment_created_timestamp) / 1e9  # Convert nanoseconds to seconds
                dt = datetime.datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
                formatted_timestamp = dt.isoformat()
                # Update the timestamp in the response to use the formatted version
                shipment_created_timestamp = formatted_timestamp
            except (ValueError, TypeError, OverflowError) as e:
                logging.info(f"Error converting timestamp: {str(e)}")
            
            trace_id = first_valid_result['sleuthTraceId']
            logging.info(f"\nUsing first valid Trace ID: {trace_id}")
            logging.info(f"\nshipment_created_timestamp is: {shipment_created_timestamp}")
            
            # Extract shipment identifier if not provided
            current_shipment_identifier = shipment_identifier
            if not current_shipment_identifier:
                body_str = first_valid_result.get('body', '')
                if body_str:
                    try:
                        body_json = json.loads(body_str)
                        if 'shipment' in body_json:
                            shipment_identifiers = body_json['shipment'].get('shipmentIdentifiers', [])
                            if shipment_identifiers:
                                current_shipment_identifier = shipment_identifiers[0].get('value')
                                logging.info(f"\nExtracted shipment identifier: {current_shipment_identifier}")
                    except json.JSONDecodeError:
                        logging.info("\nFailed to parse shipment body JSON")


            # First try with trace_id

            tracking_results = []
            try:
                tracking_results = await execute_query(start_time, end_time, 10000, "tracking", trace_id, instance=instance)
                logging.info(f"\nTracking results found using trace ID: {len(tracking_results)}")

            except Exception as e:
                logging.info(f"\nError fetching tracking method with trace ID: {str(e)}")
                raise Exception("Error fetching tracking method", str(e))
            
            # If no results with trace_id and we have shipment identifier, try alternative approach
            if not tracking_results and current_shipment_identifier:
                logging.info(f"\nNo tracking results found with trace ID. Trying with shipment identifier: {current_shipment_identifier}")
                try:
                    # Construct alternative pipeline using shipment identifier
                    alt_pipeline = (
                        "filter type = 'INTERNAL_SERVER_RESPONSE' and "
                        "requestURI = '/vendors/tl/trackingmethods/query' and "
                        f"body ~ '{current_shipment_identifier}' | "
                        "pick_col timestamp, sleuthTraceId, body | "
                        "sort desc(timestamp)"
                    )
                    tracking_results = await execute_query(
                        start_time, end_time, 10000, 
                        "tracking_status",  # Using tracking_status type to use custom pipeline
                        pipeline=alt_pipeline,
                        instance=instance
                    )
                    logging.info(f"\nTracking results found using shipment identifier: {len(tracking_results)}")
                except Exception as e:
                    logging.info(f"\nError fetching tracking method with shipment identifier: {str(e)}")
            
            # Process tracking results to get vendor type
            vendor_type = None
            if tracking_results:
                logging.info(f"\nProcessing {len(tracking_results)} tracking results..")
                for tracking_entry in tracking_results:
                    body_str = tracking_entry.get('body', '')
                    if body_str:
                        try:
                            tracking_data = json.loads(body_str)
                            if isinstance(tracking_data, list) and len(tracking_data) > 0:
                                vendor_type = tracking_data[0].get('vendorType')
                                if vendor_type:
                                    logging.info(f"\nFound vendor type: {vendor_type}")
                                    break
                                break
                        except json.JSONDecodeError:
                            continue
            
            # Extract or use provided masterShipmentId and shipment identifier
            if master_shipment_id:
                current_master_shipment_id = master_shipment_id
                current_shipment_identifier = None
                body_str = first_valid_result.get('body', '')
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
                body_str = first_valid_result.get('body', '')
                if body_str:
                    try:
                        body_json = json.loads(body_str)
                        if 'shipment' in body_json:
                            current_master_shipment_id = body_json['shipment'].get('masterShipmentId')
                    except json.JSONDecodeError:
                        pass

            # Only proceed with tracking status if we have both vendor type and master shipment id
            if vendor_type and current_master_shipment_id:
                if vendor_type == "API_PUSH":
                    if current_shipment_identifier:
                        vendor_results, carrier_results = await get_tracking_status_data(
                            start_time, end_time, vendor_type, current_master_shipment_id, 
                            current_shipment_identifier, instance=instance
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
                        
                        # Update directly in final_response - CHANGE THIS PART
                        final_response["tracking_data"] = {
                            "vendor_updates": vendor_updates,
                            "carrier_updates": carrier_updates
                        }
                    
                elif vendor_type == "MOBILE_PHONE":
                    vendor_results = await get_tracking_status_data(
                        start_time, end_time, vendor_type, current_master_shipment_id, instance=instance
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
                    
                    # Update directly in final_response - CHANGE THIS PART
                    final_response["tracking_data"] = {
                        "vendor_updates": vendor_updates
                    }
                
                else:
                    logging.info(f"\nUnsupported vendor type: {vendor_type}")
                    final_response["tracking_data"] = {"vendor_updates": []}

    # Generate Observe URLs only if we have the necessary data
    observe_urls = {}
    if vendor_type and (current_master_shipment_id or current_shipment_identifier):
        # Format times for URL
        url_start_time = start_time.strftime('%Y-%m-%dT%H.%M.%S')
        url_end_time = end_time.strftime('%Y-%m-%dT%H.%M.%S')
        
        if vendor_type == "API_PUSH":
            # Generate URL for vendor updates
            vendor_url = (
                f"https://{creds['customer_id']}.{creds['domain']}/workspace/{creds['instance_id']}/log-explorer?"
                f"datasetId={creds['dataset_id']}&"
                f"filter-requestURI=/vendors/PUSH_TRACKING/1/tl/shipments/statuses/query&"
                f"filter=body|~|{current_master_shipment_id}&"
                f"time-preset=PAST_30_DAYS&"
                f"time-start={url_start_time}%2B05.30&"
                f"time-end={url_end_time}%2B05.30"
            )
            observe_urls["vendor_updates_url"] = vendor_url

            # Generate URL for carrier updates if shipment identifier exists
            if current_shipment_identifier:
                carrier_url = (
                    f"https://{creds['customer_id']}.{creds['domain']}/workspace/{creds['instance_id']}/log-explorer?"
                    f"datasetId={creds['dataset_id']}&"
                    f"filter-requestURI=/api/v4/capacityproviders/tl/shipments/statusUpdates&"
                    f"filter=body|~|{current_shipment_identifier}&"
                    f"time-preset=PAST_30_DAYS&"
                    f"time-start={url_start_time}%2B05.30&"
                    f"time-end={url_end_time}%2B05.30"
                )
                observe_urls["carrier_updates_url"] = carrier_url

        elif vendor_type == "MOBILE_PHONE":
            # Generate URL for mobile tracking updates
            mobile_url = (
                f"https://{creds['customer_id']}.{creds['domain']}/workspace/{creds['instance_id']}/log-explorer?"
                f"datasetId={creds['dataset_id']}&"
                f"filter-requestURI=/vendors/MOBILEAPP/1/tl/shipments/statuses/query&"
                f"filter=body|~|{current_master_shipment_id}&"
                f"time-preset=PAST_30_DAYS&"
                f"time-start={url_start_time}%2B05.30&"
                f"time-end={url_end_time}%2B05.30"
            )
            observe_urls["mobile_updates_url"] = mobile_url

    # Update the final response
    final_response["shipment_created_timestamp"] = shipment_created_timestamp
    final_response["observe_urls"] = observe_urls

    print_elapsed_time(total_start_time, "\nTotal execution time")
    return json.dumps(final_response)