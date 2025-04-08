"""
Main ETL Module for TradNG

This module orchestrates the Extract, Transform, Load (ETL) process for the TradNG application.
It coordinates the flow of data between the data, logic, and presentation layers.

The ETL process follows these steps:
- Extract: Load ticker symbols from the database
- Transform: Extract financial data and clean/normalize it
- Load: Store the processed data back into the database

Architecture:
- Data Layer: Handles database operations and data persistence
- Logic Layer: Contains business logic and data processing
- Presentation Layer: Manages logging and notifications
- AI Layer: Enhances retry mechanisms and decision making

Features:
- AI-enhanced retry mechanism for API calls using LLAM 3.2
- Comprehensive error handling and logging
- Email notifications for critical failures
- Environment-specific configuration
- Database connection with intelligent retry logic

Error Handling:
- Graceful degradation on database failures (ConnectionError, SQLite3.Error)
- Email notifications for critical errors (Exception)
- Detailed logging of all operations
- AI-assisted retry timing optimization via LLAM 3.2 model
"""

from data_layer import load_tickers, load_to_db, log_decision
from logic_layer import extract_financial_data, clean_and_normalize
from presentation_layer import send_email, setup_logging
from config import get_env_config

# Setup logger
logger = setup_logging()

# Main ETL process
def main():
    """
    Main ETL (Extract, Transform, Load) process for financial data.
    
    This function orchestrates the complete ETL workflow:
    - Extracts ticker symbols from the database
    - Fetches financial data for each ticker
    - Cleans and normalizes the extracted data
    - Loads the processed data back into the database
    
    The process includes:
    - Environment-specific configuration
    - AI-enhanced retry mechanisms
    - Comprehensive error handling
    - Email notifications for failures
    - Detailed logging of all operations
    
    Returns:
        None
    
    Raises:
        ConnectionError: If database connection fails
        Exception: For any other unexpected errors during the ETL process
    """
    try:
        # Get environment-specific configuration
        env_config = get_env_config()
        logger.info(f"Starting ETL process in {env_config['debug'] and 'debug' or 'production'} mode")
        
        # Load tickers from the database using Data Layer
        logger.info("Starting ticker data retrieval from database")
        tickers = load_tickers()
        if not tickers:
            logger.error("No tickers found to process. ETL process terminated.")
            return

        # Extract financial data using Logic Layer
        logger.info("Starting financial data extraction from Yahoo Finance")
        financial_data = list(extract_financial_data(tickers))
        if not financial_data:
            logger.error("No financial data extracted. ETL process terminated.")
            return

        # Clean and normalize the extracted data
        logger.info("Starting data cleaning and normalization")
        cleaned_data = clean_and_normalize(financial_data)
        if not cleaned_data:
            logger.warning("No valid data to load after cleaning. ETL process terminated.")
            return

        # Load cleaned data to the database using Data Layer
        logger.info("Starting data loading to database")
        for record in cleaned_data:
            try:
                load_to_db(record)
            except Exception as e:
                logger.error(f"Failed to load record into the database: {record}. Error: {e}")

        logger.info("Data processing and loading completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during the ETL process: {e}")
        send_email(
            subject="ETL Process Failed",
            body=f"An error occurred during the ETL process: {e}",
            logger=logger
        )

if __name__ == "__main__":
    main()