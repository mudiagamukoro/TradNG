"""
Unit tests for the main ETL module.

This module contains test cases for the main ETL process, including the complete
workflow and error handling scenarios.
"""

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from Main_etl import main
from data_layer import ConnectionError

class TestMainETL(unittest.TestCase):
    """Test cases for the main ETL module."""

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

    @patch('Main_etl.get_env_config')
    @patch('Main_etl.load_tickers')
    @patch('Main_etl.extract_financial_data')
    @patch('Main_etl.clean_and_normalize')
    @patch('Main_etl.load_to_db')
    @patch('Main_etl.logger')
    def test_main_success(self, mock_logger, mock_load_to_db, mock_clean_and_normalize,
                         mock_extract_financial_data, mock_load_tickers, mock_get_env_config):
        """Test successful ETL process."""
        mock_get_env_config.return_value = {'debug': False}
        mock_load_tickers.return_value = self.test_tickers
        mock_extract_financial_data.return_value = self.test_financial_data
        mock_clean_and_normalize.return_value = self.test_financial_data
        
        main()
        
        mock_load_tickers.assert_called_once()
        mock_extract_financial_data.assert_called_once()
        mock_clean_and_normalize.assert_called_once()
        self.assertEqual(mock_load_to_db.call_count, 2)  # One call per record
        mock_logger.info.assert_called()

    @patch('Main_etl.get_env_config')
    @patch('Main_etl.load_tickers')
    @patch('Main_etl.logger')
    def test_main_no_tickers(self, mock_logger, mock_load_tickers, mock_get_env_config):
        """Test ETL process with no tickers."""
        mock_get_env_config.return_value = {'debug': False}
        mock_load_tickers.return_value = []
        
        main()
        
        mock_load_tickers.assert_called_once()
        mock_logger.error.assert_called_with("No tickers found to process. ETL process terminated.")

    @patch('Main_etl.get_env_config')
    @patch('Main_etl.load_tickers')
    @patch('Main_etl.extract_financial_data')
    @patch('Main_etl.logger')
    def test_main_no_financial_data(self, mock_logger, mock_extract_financial_data,
                                  mock_load_tickers, mock_get_env_config):
        """Test ETL process with no financial data."""
        mock_get_env_config.return_value = {'debug': False}
        mock_load_tickers.return_value = self.test_tickers
        mock_extract_financial_data.return_value = []
        
        main()
        
        mock_load_tickers.assert_called_once()
        mock_extract_financial_data.assert_called_once()
        mock_logger.error.assert_called_with("No financial data extracted. ETL process terminated.")

    @patch('Main_etl.get_env_config')
    @patch('Main_etl.load_tickers')
    @patch('Main_etl.extract_financial_data')
    @patch('Main_etl.clean_and_normalize')
    @patch('Main_etl.logger')
    def test_main_no_cleaned_data(self, mock_logger, mock_clean_and_normalize,
                                mock_extract_financial_data, mock_load_tickers,
                                mock_get_env_config):
        """Test ETL process with no cleaned data."""
        mock_get_env_config.return_value = {'debug': False}
        mock_load_tickers.return_value = self.test_tickers
        mock_extract_financial_data.return_value = self.test_financial_data
        mock_clean_and_normalize.return_value = []
        
        main()
        
        mock_load_tickers.assert_called_once()
        mock_extract_financial_data.assert_called_once()
        mock_clean_and_normalize.assert_called_once()
        mock_logger.warning.assert_called_with("No valid data to load after cleaning. ETL process terminated.")

    @patch('Main_etl.get_env_config')
    @patch('Main_etl.load_tickers')
    @patch('Main_etl.extract_financial_data')
    @patch('Main_etl.clean_and_normalize')
    @patch('Main_etl.load_to_db')
    @patch('Main_etl.send_email')
    @patch('Main_etl.logger')
    def test_main_database_error(self, mock_logger, mock_send_email, mock_load_to_db,
                               mock_clean_and_normalize, mock_extract_financial_data,
                               mock_load_tickers, mock_get_env_config):
        """Test ETL process with database error."""
        mock_get_env_config.return_value = {'debug': False}
        mock_load_tickers.return_value = self.test_tickers
        mock_extract_financial_data.return_value = self.test_financial_data
        mock_clean_and_normalize.return_value = self.test_financial_data
        mock_load_to_db.side_effect = ConnectionError("Database error")
        
        main()
        
        mock_load_tickers.assert_called_once()
        mock_extract_financial_data.assert_called_once()
        mock_clean_and_normalize.assert_called_once()
        mock_load_to_db.assert_called()
        mock_send_email.assert_called_once()
        mock_logger.error.assert_called()

if __name__ == '__main__':
    unittest.main() 