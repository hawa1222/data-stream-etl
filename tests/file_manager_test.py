import unittest
import os
import json
from pandas import DataFrame
from unittest.mock import patch, Mock
import logging

import sys
# Add the path to the directory containing utils.py to sys.path
sys.dont_write_bytecode = True
sys.path.append('/Users/hadid/Projects/ETL')  # Add path to system path

from utility.file_manager import FileManager  # Import your FileManager class here

# Mock the handler classes
class MockCSVHandler:
    def load(self, path, **kwargs):
        return "csv_data"

    def save(self, data, path):
        return True

class MockExcelHandler:
    def load(self, path, **kwargs):
        return "excel_data"

    def save(self, data, path):
        return True

class MockXMLHandler:
    def load(self, path, **kwargs):
        return "xml_data"

    def save(self, data, path):
        return True

class MockJSONHandler:
    def load(self, path, **kwargs):
        return "json_data"

    def save(self, data, path):
        return True

class TestFileManager(unittest.TestCase):

    # Include MockCSVHandler as an argument even if you don't use it
    @patch("utility.file_manager.CSVHandler")
    def test_load_csv(self, MockCSVHandler):
        manager = FileManager("./")
        self.assertEqual(manager.load_file("test.csv"), "csv_data")

    @patch("utility.file_manager.ExcelHandler")
    def test_load_excel(self, MockExcelHandler):
        manager = FileManager("./")
        self.assertEqual(manager.load_file("test.xls"), "excel_data")

    @patch("utility.file_manager.XMLHandler")
    def test_load_xml(self, MockXMLHandler):
        manager = FileManager("./")
        self.assertEqual(manager.load_file("test.xml"), "xml_data")

    @patch("utility.file_manager.JSONHandler")
    def test_load_json(self, MockJSONHandler):
        manager = FileManager("./")
        self.assertEqual(manager.load_file("test.json"), "json_data")

    def test_load_unsupported_file(self):
        manager = FileManager("./")
        logging.basicConfig(level=logging.ERROR)
        with self.assertLogs(level='ERROR') as log:
            self.assertIsNone(manager.load_file("test.xyz"))
            self.assertIn('ERROR:root:Unsupported file format: .xyz', log.output)

if __name__ == '__main__':
    unittest.main()
