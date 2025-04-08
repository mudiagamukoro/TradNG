"""
Unit tests for the logic layer module.

This module contains test cases for all functions in the logic_layer.py module,
including AI-powered retry timing, financial data extraction, and data cleaning.
"""

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime
import requests
from logic_layer import (
    use_llama_to_determine_retry,
    extract_financial_data,
    clean_and_normalize
)

class TestLogicLayer(unittest.TestCase):
    """Test cases for the logic layer module."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_tickers = [
            {'Ticker': 'AAPL', 'TickerID': 1},
            {'Ticker': 'GOOGL', 'TickerID': 2}
        ]
        
        self.test_financial_data = [
            {
                'TickerID': 1,
                'StartTimestamp': datetime.now(),
                'EndTimestamp': datetime.now(),
                'OpenPrice': 100.0,
                'ClosePrice': 101.0,
                'HighPrice': 102.0,
                'LowPrice': 99.0,
                'Volume': 1000
            },
            {
                'TickerID': 2,
                'StartTimestamp': datetime.now(),
                'EndTimestamp': datetime.now(),
                'OpenPrice': 200.0,
                'ClosePrice': 201.0,
                'HighPrice': 202.0,
                'LowPrice': 199.0,
                'Volume': 2000
            }
        ]

    @patch('logic_layer.requests.post')
    def test_use_llama_to_determine_retry_success(self, mock_post):
        """Test successful AI retry timing determination."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'generated_text': 'Wait for 120 seconds before retrying.'
        }
        mock_post.return_value = mock_response
        
        retry_time = use_llama_to_determine_retry(1, "10:00")
        self.assertIsInstance(retry_time, int)
        self.assertTrue(10 <= retry_time <= 300)

    @patch('logic_layer.requests.post')
    def test_use_llama_to_determine_retry_invalid_response(self, mock_post):
        """Test AI retry timing with invalid response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'generated_text': 'Invalid response without numbers'
        }
        mock_post.return_value = mock_response
        
        retry_time = use_llama_to_determine_retry(1, "10:00")
        self.assertEqual(retry_time, 60)  # Default retry time

    @patch('logic_layer.requests.post')
    def test_use_llama_to_determine_retry_api_error(self, mock_post):
        """Test AI retry timing with API error."""
        mock_post.side_effect = requests.exceptions.RequestException("API Error")
        
        retry_time = use_llama_to_determine_retry(1, "10:00")
        self.assertEqual(retry_time, 60)  # Default retry time

    @patch('logic_layer.yf.Ticker')
    @patch('logic_layer.get_database_connection')
    def test_extract_financial_data_success(self, mock_get_conn, mock_ticker):
        """Test successful financial data extraction."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        
        mock_ticker_instance = MagicMock()
        mock_ticker.return_value = mock_ticker_instance
        
        test_data = pd.DataFrame({
            'Open': [100.0],
            'Close': [101.0],
            'High': [102.0],
            'Low': [99.0],
            'Volume': [1000]
        }, index=[datetime.now()])
        
        mock_ticker_instance.history.return_value = test_data
        
        result = list(extract_financial_data(self.test_tickers))
        self.assertEqual(len(result), 2)  # One record per ticker
        self.assertIn('TickerID', result[0])
        self.assertIn('OpenPrice', result[0])

    @patch('logic_layer.yf.Ticker')
    @patch('logic_layer.get_database_connection')
    def test_extract_financial_data_empty(self, mock_get_conn, mock_ticker):
        """Test financial data extraction with no data."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        
        mock_ticker_instance = MagicMock()
        mock_ticker.return_value = mock_ticker_instance
        mock_ticker_instance.history.return_value = pd.DataFrame()
        
        result = list(extract_financial_data(self.test_tickers))
        self.assertEqual(len(result), 0)

    def test_clean_and_normalize_success(self):
        """Test successful data cleaning and normalization."""
        result = clean_and_normalize(self.test_financial_data)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['TickerID'], 1)
        self.assertEqual(result[1]['TickerID'], 2)

    def test_clean_and_normalize_invalid_data(self):
        """Test data cleaning with invalid data."""
        invalid_data = [
            {
                'TickerID': 1,
                'StartTimestamp': None,  # Invalid value
                'EndTimestamp': datetime.now(),
                'OpenPrice': 100.0,
                'ClosePrice': 101.0,
                'HighPrice': 102.0,
                'LowPrice': 99.0,
                'Volume': 1000
            }
        ]
        
        result = clean_and_normalize(invalid_data)
        self.assertEqual(len(result), 0)

if __name__ == '__main__':
    unittest.main() 