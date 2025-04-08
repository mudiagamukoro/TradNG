"""
Tests for the data layer module.

This module contains tests for the data layer functionality.
"""

import pytest
from unittest.mock import patch, MagicMock
from AgenticIntraDay.data_layer import get_database_connection, load_tickers, load_to_db, log_decision

# Test get_database_connection
@patch('pyodbc.connect')
def test_get_database_connection_success(mock_connect):
    """Test successful database connection."""
    # Setup
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    
    # Execute
    result = get_database_connection()
    
    # Assert
    assert result == mock_conn
    mock_connect.assert_called_once()

@patch('pyodbc.connect')
def test_get_database_connection_retry(mock_connect):
    """Test database connection with retry."""
    # Setup
    mock_conn = MagicMock()
    mock_connect.side_effect = [
        Exception("Connection error"),
        Exception("Connection error"),
        mock_conn
    ]
    
    # Execute
    result = get_database_connection()
    
    # Assert
    assert result == mock_conn
    assert mock_connect.call_count == 3

# Test load_tickers
@patch('AgenticIntraDay.data_layer.get_database_connection')
def test_load_tickers_success(mock_get_conn):
    """Test successful loading of tickers."""
    # Setup
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [('AAPL', 1), ('MSFT', 2)]
    mock_get_conn.return_value = mock_conn
    
    # Execute
    result = load_tickers()
    
    # Assert
    assert result == [('AAPL', 1), ('MSFT', 2)]
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()

# Test load_to_db
@patch('AgenticIntraDay.data_layer.get_database_connection')
def test_load_to_db_success(mock_get_conn):
    """Test successful loading of data to database."""
    # Setup
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_get_conn.return_value = mock_conn
    
    # Execute
    record = {
        'ticker_id': 1,
        'date': '2023-01-01',
        'open': 100.0,
        'high': 110.0,
        'low': 90.0,
        'close': 105.0,
        'volume': 1000000
    }
    load_to_db(record)
    
    # Assert
    mock_cursor.execute.assert_called_once()
    mock_conn.commit.assert_called_once()

# Test log_decision
@patch('AgenticIntraDay.data_layer.get_database_connection')
def test_log_decision_success(mock_get_conn):
    """Test successful logging of decision."""
    # Setup
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_get_conn.return_value = mock_conn
    
    # Execute
    log_decision('retry', 'Rate limit exceeded', 'Completed')
    
    # Assert
    mock_cursor.execute.assert_called_once()
    mock_conn.commit.assert_called_once() 