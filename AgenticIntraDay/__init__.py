"""
TradNG AgenticIntraDay Package

This package implements a modular ETL (Extract, Transform, Load) process
for financial data with integrated AI-powered decision making.

The package follows a layered architecture pattern:
- Data Layer: Handles database operations
- Logic Layer: Contains business logic and data processing
- Presentation Layer: Manages logging and notifications
- AI Module: Enhances the application with AI capabilities
"""

from .data_layer import get_database_connection, load_tickers, load_to_db, log_decision
from .logic_layer import extract_financial_data, clean_and_normalize, use_llama_to_determine_retry
from .presentation_layer import setup_logging, send_email
from .config import get_db_connection_string, get_env_config

__version__ = '1.0.0'

__all__ = [
    'get_database_connection',
    'load_tickers',
    'load_to_db',
    'log_decision',
    'extract_financial_data',
    'clean_and_normalize',
    'use_llama_to_determine_retry',
    'setup_logging',
    'send_email',
    'get_db_connection_string',
    'get_env_config',
] 