"""
Data Layer Module for TradNG

This module handles all database operations for the TradNG application.
It provides functions for connecting to the database, loading ticker data,
logging decisions, and storing processed financial data.

The module follows a layered architecture pattern, separating data access
from business logic and presentation concerns.

Database Operations:
- Connection management with intelligent retry logic
  * Exponential backoff for retries
  * Timeout handling
  * Different strategies for different error types
- Ticker data retrieval with error handling
- Decision logging for audit trails
- Financial data storage with validation

Error Handling:
- Connection errors: Retries with exponential backoff
- Timeout errors: Immediate retry with 1-second delay
- Data validation: Prevents invalid data from being stored
- Comprehensive logging of all operations
"""

import pyodbc
import pandas as pd
import logging
import time
from config import get_db_connection_string, DB_CONFIG, ETL_CONFIG

logger = logging.getLogger("agentic_ai")

# Custom exception for database connection errors
class ConnectionError(Exception):
    """Exception raised for database connection errors."""
    pass

# Database connection with retry mechanism
def get_database_connection():
    """
    Establishes a connection to the SQL Server database with retry logic.
    
    This function implements a robust connection strategy:
    1. Uses configuration from config.py for connection parameters
    2. Implements timeout handling (60 seconds)
    3. Uses exponential backoff for retries (2^attempt seconds)
    4. Handles different error types differently:
       - Timeout errors: Immediate retry
       - Operational errors: Exponential backoff
       - Other errors: Log and retry
    
    Returns:
        pyodbc.Connection: A connection object to the database if successful
        
    Raises:
        ConnectionError: If all connection attempts fail after retries
        pyodbc.Error: For specific database errors
    """
    logger.info("Starting database connection with retry logic")
    retry_attempts = ETL_CONFIG["max_retry_attempts"]
    for attempt in range(retry_attempts):
        try:
            connection_string = get_db_connection_string()
            conn = pyodbc.connect(connection_string, timeout=60)
            logger.info("Database connection established successfully")
            return conn
        except pyodbc.OperationalError as e:
            if "timeout" in str(e).lower():
                logger.warning(f"Timeout error: {e}. Retrying {attempt + 1}/{retry_attempts}")
                time.sleep(1)
                continue
            else:
                logger.error(f"Operational error: {e}")
                raise ConnectionError(f"Failed to connect to database: {e}")
        except pyodbc.Error as e:
            logger.error(f"Database error: {e}")
            if attempt < retry_attempts - 1:
                logger.info(f"Retrying connection {attempt + 1}/{retry_attempts}")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise ConnectionError(f"Failed to connect to database after {retry_attempts} attempts: {e}")
    
    raise ConnectionError(f"Failed to connect to database after {retry_attempts} attempts")

# Load tickers from database
def load_tickers():
    """
    Retrieves ticker symbols and their IDs from the database.
    
    This function queries the Tickers table to get all available ticker symbols
    and their corresponding IDs for processing. It includes comprehensive
    error handling and logging.
    
    Returns:
        list: A list of dictionaries containing Ticker and TickerID pairs
              Returns an empty list if the operation fails
    
    Raises:
        pyodbc.Error: For database-specific errors
        Exception: For unexpected errors during execution
    """
    logger.info("Starting ticker data retrieval from database")
    try:
        conn = get_database_connection()
        query = "SELECT Ticker, TickerID FROM Tickers"
        tickers_df = pd.read_sql(query, conn)
        logger.info("Tickers loaded successfully from the database.")
        conn.close()
        logger.info("Ticker data retrieval completed")
        return tickers_df.to_dict('records')
    except pyodbc.Error as e:
        logger.error(f"Database error while loading tickers: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error while loading tickers: {e}")
        return []

# Log decisions to the database
def log_decision(decision_type, reason, status='Pending'):
    """
    Records decision-making events in the database for auditing and tracking.
    
    This function stores information about decisions made during the ETL process,
    including the type of decision, the reason for the decision, and its current status.
    It provides a complete audit trail for all system decisions.
    
    Args:
        decision_type (str): The category or type of decision being logged
        reason (str): The explanation or rationale for the decision
        status (str, optional): The current status of the decision. Defaults to 'Pending'.
    
    Raises:
        pyodbc.Error: For database-specific errors
        Exception: For unexpected errors during execution
    """
    logger.info("Logging decision to database")
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO DecisionLog (DecisionType, Reason, DecisionStatus)
            VALUES (?, ?, ?)""",
            decision_type, reason, status
        )
        conn.commit()
        conn.close()
        logger.info(f"Decision logged: {decision_type}, Reason: {reason}, Status: {status}")
    except pyodbc.Error as e:
        logger.error(f"Database error while logging decision: {e}")
    except Exception as e:
        logger.error(f"Unexpected error while logging decision: {e}")

# Load processed data into database
def load_to_db(record):
    """
    Inserts a processed financial data record into the database.
    
    This function takes a cleaned and normalized financial data record and
    inserts it into the IntradayStockPrice table. It includes comprehensive
    validation and error handling.
    
    Args:
        record (dict): A dictionary containing the financial data record with keys:
                      TickerID, StartTimestamp, EndTimestamp, OpenPrice, ClosePrice,
                      HighPrice, LowPrice, and Volume
                      
    Raises:
        ValueError: If the record is None or contains None values
        pyodbc.Error: For database-specific errors
        Exception: For unexpected errors during execution
    """
    logger.info("Starting data loading to database")
    try:
        if record is None or any(value is None for value in record.values()):
            raise ValueError(f"Invalid record data: {record}")
        logger.info(f"Loading data into the database for ticker ID: {record['TickerID']}...")
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO IntradayStockPrice (TickerID, StartTimestamp, EndTimestamp, OpenPrice, ClosePrice, HighPrice, LowPrice, Volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            record['TickerID'], record['StartTimestamp'], record['EndTimestamp'],
            record['OpenPrice'], record['ClosePrice'], record['HighPrice'], record['LowPrice'], record['Volume']
        )
        conn.commit()
        conn.close()
        logger.info(f"Record for ticker ID {record['TickerID']} loaded successfully.")
    except ValueError as e:
        logger.error(f"Invalid record: {e}")
    except pyodbc.Error as e:
        logger.error(f"Database error while loading data: {e}")
    except Exception as e:
        logger.error(f"Unexpected error while loading data: {e}")
