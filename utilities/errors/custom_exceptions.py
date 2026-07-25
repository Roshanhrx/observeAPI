import logging
import sys
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout) 
    ]
)
logger = logging.getLogger(__name__)


em = {
      400: {"error":  "BAD_REQUEST", "message":"The request is not valid" }, 
      507: {"error":  "DB_CONNECT_ERROR", "message":"Error occured while making cosmos db connection or while fetching the cosmos data" },
      508: {"error":  "POSTGRES_DB_CONNECT_ERROR", "message":"Error occured while making postgres db connection or while fetching the postgres data" },
      2001: {"error":  "EXPIRED_TOKEN_ERROR", "message":"The token provided has expired" },
      2002: {"error":  "INVALID_TOKEN_ERROR", "message":"The token provided is not valid" },
     }

class CustomException(Exception):
    """Base custom exception class."""
    
    def __init__(self, status_code, input_value):
        super().__init__(em[status_code]["error"], em[status_code]["message"], input_value)
        self.status_code = status_code
        self.detail = input_value
        logger.error(self.detail)


class CustomInternalServerError(CustomException):
    """Exception for invalid input."""
    def __init__(self, input_value):
        super().__init__(500, input_value)


class InvalidInputException(CustomException):
    """Exception for invalid input."""
    def __init__(self, input_value):
        super().__init__(400, input_value)
        logger.error(f"InvalidInputException : {input_value}")
       

class CustomDatabaseException(CustomException):
    """Exception for cosmos database-related errors."""
    def __init__(self, database_name, input_value):
        super().__init__(507, input_value)
        logger.error(f"Database error in {database_name}: {input_value}")

class CustomPostgresDatabaseException(CustomException):
    """Exception for postgres database-related errors."""
    def __init__(self, database_name, input_value):
        super().__init__(508, input_value)
        logger.error(f"Database error in postgres db {database_name}: {input_value}")

class CustomTokenExpiredException(CustomException):
    """Exception for a token that has expired"""
    def __init__(self, input_value):
        super().__init__(2001, input_value)
        logger.error(f"Provided token has expired: {input_value}")

class CustomInvalidTokenException(CustomException):
    """Exception for a token that has expired"""
    def __init__(self, input_value):
        super().__init__(2002, input_value)
        logger.error(f"Provided token is invalid: {input_value}")

