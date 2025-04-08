"""
Logic Layer Module for TradNG

This module contains the business logic for the TradNG application.
It handles data extraction from external sources, data cleaning and normalization,
and implements intelligent retry mechanisms using AI for API rate limiting.

The module follows a layered architecture pattern, separating business logic
from data access and presentation concerns.
"""

import yfinance as yf
import logging
import requests
import json
import re
from datetime import datetime
import time
import pandas as pd
from data_layer import get_database_connection, log_decision, ConnectionError
from config import API_CONFIG, AI_CONFIG, ETL_CONFIG

logger = logging.getLogger("agentic_ai")

# Use LLAM 3.2 to determine retry timing
def use_llama_to_determine_retry(attempt, current_time):
    """
    Uses the LLAM 3.2 AI model to intelligently determine optimal retry timing.
    
    This function queries the LLAM 3.2 model via the Hugging Face API to get
    recommendations on when to retry API calls that have been rate-limited.
    
    Args:
        attempt (int): The current attempt number (1-based)
        current_time (str): The current time in HH:MM format
        
    Returns:
        int: The recommended wait time in seconds before retrying
    """
    logger.info("Using LLAM 3.2 to determine retry timing")
    try:
        headers = {
            'Authorization': f'Bearer {API_CONFIG["huggingface_key"]}',
            'Content-Type': 'application/json'
        }
        data = {
            "inputs": f"I have attempted to retrieve data from the Yahoo Finance API {attempt} times, and each attempt has been throttled due to metering limits. The error received suggests that the rate limit has been exceeded. Current time is {current_time}. When should I retry to avoid being throttled again? Please provide the suggested wait time in seconds.",
            "parameters": {
                "temperature": AI_CONFIG.get("temperature", 0.7)
            }
        }
        response = requests.post(
            'https://api-inference.huggingface.co/models/meta-llama/Llama-2-7b-chat-hf',
            headers=headers,
            data=json.dumps(data)
        )
        response_data = response.json()
        if response.status_code == 200 and 'generated_text' in response_data:
            retry_time_text = response_data['generated_text']
            logger.info(f"LLAM 3.2 suggests retry after: {retry_time_text}")
            
            # Extract numeric value from the response
            numeric_match = re.search(r'\d+', retry_time_text)
            if numeric_match:
                retry_time = int(numeric_match.group())
                # Ensure reasonable retry time (between min and max from config)
                retry_time = max(ETL_CONFIG["min_retry_time"], min(retry_time, ETL_CONFIG["max_retry_time"]))
                logger.info(f"Using retry time of {retry_time} seconds")
                return retry_time
            else:
                logger.warning("Could not extract numeric value from AI response, using default")
                return ETL_CONFIG["default_retry_time"]
        else:
            logger.error(f"Failed to get response from LLAM 3.2: {response_data}")
            return ETL_CONFIG["default_retry_time"]
    except Exception as e:
        logger.error(f"Error while querying LLAM 3.2: {e}")
        return ETL_CONFIG["default_retry_time"]

# Extract financial data from Yahoo Finance with retry mechanism
def extract_financial_data(tickers):
    """
    Extracts financial data from Yahoo Finance for the given tickers.
    
    This function retrieves historical intraday stock data for each ticker,
    implementing intelligent retry logic with AI-powered timing recommendations
    when API rate limits are encountered.
    
    Args:
        tickers (list): List of dictionaries containing Ticker and TickerID pairs
        
    Yields:
        dict: A dictionary containing processed financial data for each time period
              with keys: TickerID, StartTimestamp, EndTimestamp, OpenPrice, ClosePrice,
              HighPrice, LowPrice, and Volume
    """
    logger.info("Starting financial data extraction from Yahoo Finance")
    retry_attempts = ETL_CONFIG["max_retry_attempts"]
    for ticker_info in tickers:
        ticker = ticker_info.get('Ticker')
        ticker_id = ticker_info.get('TickerID')
        if ticker is None or ticker_id is None:
            logger.warning(f"Invalid ticker information: {ticker_info}")
            continue

        # Check the latest entry in the database for this ticker
        start_date = ETL_CONFIG["default_start_date"]
        try:
            conn = get_database_connection()
            if conn is None:
                raise ConnectionError("Database connection is None")
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(EndTimestamp) FROM IntradayStockPrice WHERE TickerID = ?", ticker_id)
            last_timestamp = cursor.fetchone()[0]
            conn.close()
            if last_timestamp:
                start_date = last_timestamp.strftime('%Y-%m-%d')
        except Exception as e:
            logger.error(f"Error while checking latest timestamp for ticker {ticker}: {e}")
            continue

        # Retry mechanism for data extraction
        for attempt in range(retry_attempts):
            try:
                logger.info(f"Extracting data for ticker: {ticker} using Yahoo Finance starting from {start_date}")
                ticker_data = yf.Ticker(ticker)
                historical_data = ticker_data.history(
                    start=start_date, 
                    period=ETL_CONFIG["data_period"], 
                    interval=ETL_CONFIG["data_interval"]
                )

                if historical_data is None or historical_data.empty:
                    logger.warning(f"No data available for ticker: {ticker}")
                    break

                logger.info(f"Data extraction successful for ticker: {ticker}")
                for index, row in historical_data.iterrows():
                    start_timestamp = index
                    end_timestamp = start_timestamp + pd.Timedelta(hours=1)
                    yield {
                        'TickerID': ticker_id,
                        'StartTimestamp': start_timestamp,
                        'EndTimestamp': end_timestamp,
                        'OpenPrice': row.get('Open', None),
                        'ClosePrice': row.get('Close', None),
                        'HighPrice': row.get('High', None),
                        'LowPrice': row.get('Low', None),
                        'Volume': row.get('Volume', None)
                    }
                break
            except requests.exceptions.RequestException as e:
                if attempt < retry_attempts - 1:
                    current_time = datetime.now().strftime('%H:%M %Z')
                    logger.error(f"API throttling error for ticker {ticker}: {e}. Using LLAM 3.2 to determine retry timing")
                    retry_time = use_llama_to_determine_retry(attempt + 1, current_time)
                    time.sleep(retry_time)
                else:
                    log_decision("RetryExtractData", f"Maximum retry attempts reached for Yahoo Finance API for ticker {ticker}. Requesting permission to retry.")
                    break
            except Exception as e:
                logger.error(f"Unexpected error during data extraction for ticker {ticker}: {e}")
                if attempt < retry_attempts - 1:
                    logger.info(f"Retrying {attempt + 1}/{retry_attempts}")
                    time.sleep(ETL_CONFIG["default_retry_time"])
                else:
                    log_decision("RetryExtractData", f"Maximum retry attempts reached for unexpected error during data extraction for ticker {ticker}. Requesting permission to retry.")
                    break
    logger.info("Financial data extraction completed")

# Data Cleaning and Normalization
def clean_and_normalize(data):
    """
    Cleans and normalizes financial data before database insertion.
    
    This function filters out records with missing or invalid values and
    ensures data consistency before loading into the database.
    
    Args:
        data (list): List of dictionaries containing financial data records
        
    Returns:
        list: A list of cleaned and validated financial data records
    """
    logger.info("Starting data cleaning and normalization")
    cleaned_data = []
    try:
        for record in data:
            if all(value is not None for value in record.values()):
                cleaned_data.append(record)
            else:
                logger.warning(f"Incomplete data found and excluded: {record}")
        logger.info("Data cleaning and normalization completed successfully")
    except Exception as e:
        logger.error(f"Error during data cleaning and normalization: {e}")
    return cleaned_data
