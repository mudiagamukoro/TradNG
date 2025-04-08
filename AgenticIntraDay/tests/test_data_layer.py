"""
Unit tests for the data layer module.

This module contains test cases for all functions in the data_layer.py module,
including database connection, ticker loading, decision logging, and data loading.
"""

import unittest
from unittest.mock import patch, MagicMock
import pyodbc
import pandas as pd
from datetime import datetime
from data_layer import (
    get_database_connection,
    load_tickers,
    log_decision,
    load_to_db,
    ConnectionError
)

class TestDataLayer(unittest.TestCase):
    """Test cases for the data layer module."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

    @patch('data_layer.pyodbc.connect')
    def test_get_database_connection_success(self, mock_connect):
        """Test successful database connection."""
        mock_connect.return_value = self.mock_conn
        conn = get_database_connection()
        self.assertEqual(conn, self.mock_conn)
        mock_connect.assert_called_once()

    @patch('data_layer.pyodbc.connect')
    def test_get_database_connection_timeout(self, mock_connect):
        """Test database connection with timeout error."""
        mock_connect.side_effect = pyodbc.OperationalError("timeout")
        with self.assertRaises(ConnectionError):
            get_database_connection()

    @patch('data_layer.get_database_connection')
    def test_load_tickers_success(self, mock_get_conn):
        """Test successful ticker loading."""
        mock_get_conn.return_value = self.mock_conn
        test_data = pd.DataFrame({
            'Ticker': ['AAPL', 'GOOGL'],
            'TickerID': [1, 2]
        })
        self.mock_cursor.fetchall.return_value = test_data.values.tolist()
        
        result = load_tickers()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['Ticker'], 'AAPL')
        self.assertEqual(result[0]['TickerID'], 1)

    @patch('data_layer.get_database_connection')
    def test_load_tickers_empty(self, mock_get_conn):
        """Test ticker loading with no data."""
        mock_get_conn.return_value = self.mock_conn
        self.mock_cursor.fetchall.return_value = []
        
        result = load_tickers()
        self.assertEqual(result, [])

    @patch('data_layer.get_database_connection')
    def test_log_decision_success(self, mock_get_conn):
        """Test successful decision logging."""
        mock_get_conn.return_value = self.mock_conn
        
        log_decision("TestDecision", "Test reason", "Completed")
        
        self.mock_cursor.execute.assert_called_once()
        self.mock_conn.commit.assert_called_once()

    @patch('data_layer.get_database_connection')
    def test_load_to_db_success(self, mock_get_conn):
        """Test successful data loading."""
        mock_get_conn.return_value = self.mock_conn
        
        test_record = {
            'TickerID': 1,
            'StartTimestamp': datetime.now(),
            'EndTimestamp': datetime.now(),
            'OpenPrice': 100.0,
            'ClosePrice': 101.0,
            'HighPrice': 102.0,
            'LowPrice': 99.0,
            'Volume': 1000
        }
        
        load_to_db(test_record)
        
        self.mock_cursor.execute.assert_called_once()
        self.mock_conn.commit.assert_called_once()

    @patch('data_layer.get_database_connection')
    def test_load_to_db_invalid_record(self, mock_get_conn):
        """Test data loading with invalid record."""
        mock_get_conn.return_value = self.mock_conn
        
        test_record = {
            'TickerID': 1,
            'StartTimestamp': None,  # Invalid value
            'EndTimestamp': datetime.now(),
            'OpenPrice': 100.0,
            'ClosePrice': 101.0,
            'HighPrice': 102.0,
            'LowPrice': 99.0,
            'Volume': 1000
        }
        
        with self.assertRaises(ValueError):
            load_to_db(test_record)

if __name__ == '__main__':
    unittest.main() 