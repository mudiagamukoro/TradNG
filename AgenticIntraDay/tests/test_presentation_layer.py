"""
Unit tests for the presentation layer module.

This module contains test cases for all functions in the presentation_layer.py module,
including logging setup and email notifications.
"""

import unittest
from unittest.mock import patch, MagicMock
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from presentation_layer import setup_logging, send_email

class TestPresentationLayer(unittest.TestCase):
    """Test cases for the presentation layer module."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_subject = "Test Subject"
        self.test_body = "Test Body"
        self.test_logger = MagicMock(spec=logging.Logger)

    @patch('presentation_layer.logging.getLogger')
    @patch('presentation_layer.logging.FileHandler')
    @patch('presentation_layer.logging.StreamHandler')
    def test_setup_logging_success(self, mock_stream_handler, mock_file_handler, mock_get_logger):
        """Test successful logging setup."""
        mock_logger = MagicMock(spec=logging.Logger)
        mock_get_logger.return_value = mock_logger
        
        mock_file_handler_instance = MagicMock()
        mock_file_handler.return_value = mock_file_handler_instance
        
        mock_stream_handler_instance = MagicMock()
        mock_stream_handler.return_value = mock_stream_handler_instance
        
        logger = setup_logging()
        
        self.assertEqual(logger, mock_logger)
        mock_logger.addHandler.assert_called()
        mock_logger.setLevel.assert_called_once()

    @patch('presentation_layer.logging.getLogger')
    def test_setup_logging_failure(self, mock_get_logger):
        """Test logging setup with error."""
        mock_get_logger.side_effect = Exception("Logging setup failed")
        
        with self.assertRaises(Exception):
            setup_logging()

    @patch('presentation_layer.smtplib.SMTP')
    def test_send_email_success(self, mock_smtp):
        """Test successful email sending."""
        mock_smtp_instance = MagicMock()
        mock_smtp.return_value = mock_smtp_instance
        
        result = send_email(self.test_subject, self.test_body, self.test_logger)
        
        self.assertTrue(result)
        mock_smtp_instance.starttls.assert_called_once()
        mock_smtp_instance.login.assert_called_once()
        mock_smtp_instance.sendmail.assert_called_once()
        mock_smtp_instance.quit.assert_called_once()
        self.test_logger.info.assert_called_once()

    @patch('presentation_layer.smtplib.SMTP')
    def test_send_email_smtp_error(self, mock_smtp):
        """Test email sending with SMTP error."""
        mock_smtp_instance = MagicMock()
        mock_smtp_instance.starttls.side_effect = smtplib.SMTPException("SMTP Error")
        mock_smtp.return_value = mock_smtp_instance
        
        result = send_email(self.test_subject, self.test_body, self.test_logger)
        
        self.assertFalse(result)
        self.test_logger.error.assert_called_once()

    @patch('presentation_layer.smtplib.SMTP')
    def test_send_email_login_error(self, mock_smtp):
        """Test email sending with login error."""
        mock_smtp_instance = MagicMock()
        mock_smtp_instance.login.side_effect = smtplib.SMTPAuthenticationError("Login failed")
        mock_smtp.return_value = mock_smtp_instance
        
        result = send_email(self.test_subject, self.test_body, self.test_logger)
        
        self.assertFalse(result)
        self.test_logger.error.assert_called_once()

    def test_send_email_no_logger(self):
        """Test email sending without provided logger."""
        with patch('presentation_layer.smtplib.SMTP') as mock_smtp:
            mock_smtp_instance = MagicMock()
            mock_smtp.return_value = mock_smtp_instance
            
            result = send_email(self.test_subject, self.test_body)
            
            self.assertTrue(result)
            mock_smtp_instance.starttls.assert_called_once()
            mock_smtp_instance.login.assert_called_once()
            mock_smtp_instance.sendmail.assert_called_once()
            mock_smtp_instance.quit.assert_called_once()

if __name__ == '__main__':
    unittest.main() 