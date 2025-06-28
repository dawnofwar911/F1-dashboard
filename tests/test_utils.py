import unittest
from unittest.mock import patch, mock_open, MagicMock
import os
import json
import pandas as pd
from pathlib import Path

from app import utils, config

class TestUtils(unittest.TestCase):

    @patch('utils.config.SETTINGS_FILE_PATH')
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_global_settings_file_exists(self, mock_json_load, mock_file_open, mock_settings_file_path):
        mock_settings_file_path.exists.return_value = True
        mock_json_load.return_value = {'record_live_sessions': True, 'theme': 'dark'}

        settings = utils.load_global_settings()

        mock_settings_file_path.exists.assert_called_once()
        mock_file_open.assert_called_once_with(mock_settings_file_path, 'r')
        mock_json_load.assert_called_once_with(mock_file_open())
        self.assertEqual(settings, {'record_live_sessions': True, 'theme': 'dark'})

    @patch('utils.config.SETTINGS_FILE_PATH')
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load', side_effect=json.JSONDecodeError("Test Error", "doc", 0))
    def test_load_global_settings_json_error(self, mock_json_load, mock_file_open, mock_settings_file_path):
        mock_settings_file_path.exists.return_value = True

        settings = utils.load_global_settings()

        mock_settings_file_path.exists.assert_called_once()
        mock_file_open.assert_called_once_with(mock_settings_file_path, 'r')
        mock_json_load.assert_called_once_with(mock_file_open())
        self.assertEqual(settings, {'record_live_sessions': False})

    @patch('utils.config.SETTINGS_FILE_PATH')
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.dump')
    def test_save_global_settings(self, mock_json_dump, mock_file_open, mock_settings_file_path):
        settings_to_save = {'record_live_sessions': True, 'theme': 'light'}

        utils.save_global_settings(settings_to_save)

        mock_file_open.assert_called_once_with(mock_settings_file_path, 'w')
        mock_json_dump.assert_called_once_with(settings_to_save, mock_file_open(), indent=4)

    def test_convert_kph_to_mph_single_value(self):
        config.KPH_TO_MPH_FACTOR = 0.621371
        self.assertAlmostEqual(utils.convert_kph_to_mph(100), 62.1371)
        self.assertAlmostEqual(utils.convert_kph_to_mph(0), 0)

    def test_convert_kph_to_mph_list(self):
        config.KPH_TO_MPH_FACTOR = 0.621371
        result = utils.convert_kph_to_mph([100, 200, 50])
        self.assertIsInstance(result, list)
        self.assertAlmostEqual(result[0], 62.1371)
        self.assertAlmostEqual(result[1], 124.2742)
        self.assertAlmostEqual(result[2], 31.06855)

    def test_convert_kph_to_mph_pandas_series(self):
        config.KPH_TO_MPH_FACTOR = 0.621371
        series = pd.Series([100, 200, 50])
        result = utils.convert_kph_to_mph(series)
        self.assertIsInstance(result, pd.Series)
        self.assertAlmostEqual(result.iloc[0], 62.1371)
        self.assertAlmostEqual(result.iloc[1], 124.2742)
        self.assertAlmostEqual(result.iloc[2], 31.06855)

    def test_convert_kph_to_mph_none_input(self):
        self.assertIsNone(utils.convert_kph_to_mph(None))

    def test_convert_kph_to_mph_unsupported_type(self):
        # Test with a string, which should trigger the warning and return original
        with self.assertLogs('F1App.Utils', level='WARNING') as cm:
            result = utils.convert_kph_to_mph("not_a_number")
            self.assertEqual(result, "not_a_number")
            self.assertIn("received an unexpected type", cm.output[0])

    def test_determine_session_type_from_name(self):
        self.assertEqual(utils.determine_session_type_from_name("Free Practice 1"), config.SESSION_TYPE_PRACTICE)
        self.assertEqual(utils.determine_session_type_from_name("Qualifying"), config.SESSION_TYPE_QUALI)
        self.assertEqual(utils.determine_session_type_from_name("Sprint Race"), config.SESSION_TYPE_SPRINT)
        self.assertEqual(utils.determine_session_type_from_name("Grand Prix"), "Unknown")
        self.assertEqual(utils.determine_session_type_from_name("Unknown Session"), "Unknown")
        self.assertEqual(utils.determine_session_type_from_name("Pre-Race Show"), "Unknown")
        self.assertEqual(utils.determine_session_type_from_name("Sprint Qualifying"), config.SESSION_TYPE_QUALI)

if __name__ == '__main__':
    unittest.main()
