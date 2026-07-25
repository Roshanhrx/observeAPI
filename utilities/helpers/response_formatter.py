import datetime

class ResponseFormatter:

    def format_success_response(self,data,message,status_code=200):
        timestamp = datetime.datetime.utcnow().isoformat() + "Z"
        response = {
            "timestamp": timestamp,
            "status": status_code,
            "message": message,
            "data": data
        }
        return response

    def format_error_response(self,error_obj):
        timestamp = datetime.datetime.utcnow().isoformat() + "Z"
 
        response = {
            "timestamp": timestamp,
            "status": error_obj.status_code if hasattr(error_obj, 'status_code') and error_obj.status_code is not None else 500,
            "error": error_obj.args[0] ,
            "message": error_obj.args[1] if len(error_obj.args) > 1 and error_obj.args[1] is not None else "Internal Server Error",
            "detail": error_obj.args[2] if len(error_obj.args) > 2  and error_obj.args[2] is not None else "An unexpected error occured while executing the code"
        }
        return response
