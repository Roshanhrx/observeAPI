import logging
import sys
import os
import json
from dotenv import load_dotenv
from utilities.errors.custom_exceptions import InvalidInputException
import uvicorn  # pyright: ignore[reportMissingImports]
import aiohttp  # pyright: ignore[reportMissingImports]
import asyncio
import time
import uuid
from datetime import datetime
from fastapi import FastAPI, Request, Response, HTTPException  # pyright: ignore[reportMissingImports]
from fastapi.middleware.cors import CORSMiddleware  # pyright: ignore[reportMissingImports]
from fastapi.responses import StreamingResponse, JSONResponse  # pyright: ignore[reportMissingImports]
from utilities.helpers.response_formatter import ResponseFormatter
from utilities.helpers.observe_truckload_tracking import fetch_all_data
from utilities.helpers.observe_ratequote import fetch_ltl_quote_data
from utilities.helpers.observe_shipment_creation import fetch_shipment_creation
from utilities.helpers.observe_error_logs import fetch_error_logs

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout) 
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI()
response_formatter = ResponseFormatter()
mime_type = os.environ.get('CUSTOM_RESPONSE_TYPE', 'application/json')

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust origins as needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def read_root() -> Response:
    payload = {"Health": "ok"}
    return Response(content=json.dumps(payload), media_type=mime_type, status_code=200)


@app.get("/iptest")
async def ip_test():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.ipify.org?format=json") as resp:
            txt = await resp.text()
            return {"ip": txt}

@app.post("/observe/truckload/tracking")
async def observe_truckload_tracking(request: Request):
    logger.info('Observe flow starting....')
    try:
        req_body = await request.json()
        
        master_shipment_id = req_body.get('master_shipment_id')
        shipment_identifier = req_body.get('shipment_identifier')
        carrier_identifier = req_body.get('carrier_identifier')
        instance = req_body.get('instance', 'NA')  # Default to NA if not specified
        
        if master_shipment_id is None:
            updated_list = await fetch_all_data(shipment_identifier=shipment_identifier, carrier_identifier=carrier_identifier, instance=instance)
        else:
            updated_list = await fetch_all_data(master_shipment_id=master_shipment_id, instance=instance)
            
        response = response_formatter.format_success_response(updated_list, "successfully fetched observe data", 200)
        return JSONResponse(content=response, media_type=mime_type, status_code=200)
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        status_code = 500
        if hasattr(e, 'status_code') and e.status_code == 401:
            status_code = 401
            
        response = response_formatter.format_error_response(e)
        return JSONResponse(
            content=response,
            media_type=mime_type,
            status_code=status_code
        )

@app.post("/observe/ltl/quotes")
async def observe_ltl_quotes(request: Request):
    logger.info('Observe LTL quotes flow starting....')
    try:
        req_body = await request.json()
        rate_quote_id = req_body.get('rate_quote_id')
        
        if rate_quote_id is None:
            raise InvalidInputException("rate_quote_id is required")
            
        result = await fetch_ltl_quote_data(rate_quote_id=rate_quote_id)
        response = response_formatter.format_success_response(result, "successfully fetched observe ltl quote data", 200)
        return JSONResponse(content=response, media_type=mime_type, status_code=200)
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        status_code = 500
        if hasattr(e, 'status_code') and e.status_code == 401:
            status_code = 401
            
        response = response_formatter.format_error_response(e)
        return JSONResponse(
            content=response,
            media_type=mime_type,
            status_code=status_code
        )

@app.post("/observe/shipment/creation")
async def observe_shipment_creation(request: Request):
    logger.info('Observe shipment creation flow starting....')
    try:
        req_body = await request.json()
        
        master_shipment_id = req_body.get('master_shipment_id')
        shipment_identifier = req_body.get('shipment_identifier')
        instance = req_body.get('instance', 'NA')  # Default to NA if not specified
        
        # Validate input
        if not (master_shipment_id or shipment_identifier):
            raise InvalidInputException("Either master_shipment_id OR shipment_identifier must be provided")
        
        # Call the new function with appropriate parameters
        if master_shipment_id:
            result = await fetch_shipment_creation(master_shipment_id=master_shipment_id, instance=instance)
        else:
            result = await fetch_shipment_creation(
                shipment_identifier=shipment_identifier,
                instance=instance
            )
            
        response = response_formatter.format_success_response(
            result, 
            "Successfully fetched shipment creation data", 
            200
        )
        return JSONResponse(content=response, media_type=mime_type, status_code=200)
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        status_code = 500
        if hasattr(e, 'status_code') and e.status_code == 401:
            status_code = 401
            
        response = response_formatter.format_error_response(e)
        return JSONResponse(
            content=response,
            media_type=mime_type,
            status_code=status_code
        )

@app.post("/observe/error/logs")
async def observe_error_logs(request: Request):
    logger.info('Observe error logs flow starting....')
    try:
        req_body = await request.json()
        error_id = req_body.get('error_id')
        instance = req_body.get('instance', 'NA')  # Default to NA if not specified
        
        # Validate input
        if not error_id:
            raise InvalidInputException("error_id must be provided")
        
        result = await fetch_error_logs(error_id=error_id, instance=instance)
        response = response_formatter.format_success_response(
            result, 
            "Successfully fetched error log data", 
            200
        )
        return JSONResponse(content=response, media_type=mime_type, status_code=200)
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        status_code = 500
        if hasattr(e, 'status_code') and e.status_code == 401:
            status_code = 401
            
        response = response_formatter.format_error_response(e)
        return JSONResponse(
            content=response,
            media_type=mime_type,
            status_code=status_code
        )

if __name__ == "__main__":
    uvicorn.run('main:app', host='0.0.0.0', port=3100)
