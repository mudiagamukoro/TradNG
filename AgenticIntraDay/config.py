"""
Configuration Module for TradNG

This module contains all configuration settings for the TradNG application.
It includes settings for API keys, database connections, ETL parameters,
and AI model configurations.
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Configuration
API_CONFIG = {
    "huggingface_key": os.getenv("HUGGINGFACE_API_KEY"),
    "yahoo_finance_rate_limit": int(os.getenv("YAHOO_FINANCE_RATE_LIMIT", "2000")),
}

# AI Model Configuration
AI_CONFIG = {
    "model": "meta-llama/Llama-2-7b-chat-hf",
    "temperature": float(os.getenv("AI_TEMPERATURE", "0.7")),
    "max_tokens": int(os.getenv("AI_MAX_TOKENS", "100")),
}

# ETL Configuration
ETL_CONFIG = {
    "max_retry_attempts": int(os.getenv("MAX_RETRY_ATTEMPTS", "5")),
    "default_retry_time": int(os.getenv("DEFAULT_RETRY_TIME", "60")),
    "min_retry_time": int(os.getenv("MIN_RETRY_TIME", "10")),
    "max_retry_time": int(os.getenv("MAX_RETRY_TIME", "300")),
    "default_start_date": os.getenv("DEFAULT_START_DATE", "2014-11-01"),
    "data_interval": os.getenv("DATA_INTERVAL", "1h"),
    "data_period": os.getenv("DATA_PERIOD", "1d"),
}

# Database Configuration
DB_CONFIG = {
    "server": os.getenv("DB_SERVER", "localhost"),
    "database": os.getenv("DB_NAME", "tradng"),
    "username": os.getenv("DB_USERNAME", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "driver": os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server"),
}

# Email Configuration
EMAIL_CONFIG = {
    "smtp_server": os.getenv("SMTP_SERVER", "smtp.gmail.com"),
    "smtp_port": int(os.getenv("SMTP_PORT", "587")),
    "smtp_sender_email": os.getenv("SENDER_EMAIL"),
    "smtp_sender_password": os.getenv("SENDER_PASSWORD"),
    "notification_email": os.getenv("RECIPIENT_EMAIL"),
}

# Logging Configuration
LOG_CONFIG = {
    "log_level": os.getenv("LOG_LEVEL", "INFO"),
    "log_file": os.getenv("LOG_FILE", "tradng.log"),
    "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
}

def get_db_connection_string():
    """
    Returns a database connection string based on the configuration.
    
    This function constructs a connection string using either the default
    configuration values or overridden values from environment variables.
    
    Returns:
        str: A database connection string formatted for pyodbc.
    """
    return (
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
    )

def get_env_config():
    """
    Returns the environment configuration.
    
    Returns:
        dict: A dictionary containing environment-specific settings
    """
    return {
        "debug": os.getenv("DEBUG", "False").lower() == "true",
        "environment": os.getenv("ENVIRONMENT", "production"),
    } 