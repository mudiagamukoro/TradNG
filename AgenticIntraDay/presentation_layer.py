"""
Presentation Layer Module for TradNG

This module handles the presentation and notification aspects of the TradNG application.
It provides functions for setting up logging and sending email notifications.

The module follows a layered architecture pattern, separating presentation concerns
from data access and business logic.
"""

import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import LOG_CONFIG, EMAIL_CONFIG, get_env_config

# Setup logging to handle both console and file
def setup_logging():
    """
    Configures the application's logging system.
    
    This function sets up a logger that writes to both a file and the console.
    The file handler captures DEBUG level messages, while the console handler
    captures INFO level messages for user feedback during execution.
    
    Returns:
        logging.Logger: A configured logger instance
        
    Raises:
        Exception: If logging setup fails
    """
    try:
        logger = logging.getLogger("agentic_ai")
        
        # Get environment-specific configuration
        env_config = get_env_config()
        log_level = getattr(logging, LOG_CONFIG['log_level'])
        logger.setLevel(log_level)

        # File handler to log into a file
        file_handler = logging.FileHandler(LOG_CONFIG['log_file'])
        file_handler.setLevel(logging.DEBUG)

        # Console handler to print to console during testing
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Logging format
        formatter = logging.Formatter(LOG_CONFIG['log_format'])
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    except Exception as e:
        print(f"Failed to setup logging: {e}")
        raise

# Send email notification
def send_email(subject, body, logger=None):
    """
    Sends an email notification using SMTP.
    
    This function creates and sends an email with the specified subject and body.
    It uses the configured SMTP server and credentials from the configuration module.
    
    Args:
        subject (str): The subject of the email
        body (str): The body of the email
        logger (logging.Logger, optional): A logger instance for logging email operations
        
    Returns:
        bool: True if the email was sent successfully, False otherwise
        
    Raises:
        Exception: If email sending fails
    """
    if logger is None:
        logger = logging.getLogger("agentic_ai")
        
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG['smtp_sender_email']
        msg['To'] = EMAIL_CONFIG['notification_email']
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()  # Enable TLS
        server.login(EMAIL_CONFIG['smtp_sender_email'], EMAIL_CONFIG['smtp_sender_password'])
        server.sendmail(EMAIL_CONFIG['smtp_sender_email'], EMAIL_CONFIG['notification_email'], msg.as_string())
        server.quit()
        
        logger.info(f"Email sent successfully: {subject}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False
