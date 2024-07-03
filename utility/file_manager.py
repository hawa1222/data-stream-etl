import json
import os
import xml.etree.ElementTree as ET

import pandas as pd
from bs4 import BeautifulSoup

from utility.logging import setup_logging

logger = setup_logging()


class FileManager:
    """
    Class to load and save files in different formats

    Supported formats:
    - CSV
    - Excel
    - JSON
    - XML
    - HTML
    """

    def __init__(self):
        pass

    def _get_full_path(self, directory, file_name):
        return os.path.join(directory, file_name)

    def load_file(self, directory, file_name, **kwargs):
        """
        Load a file from the specified directory with the given file name.

        Parameters:
        - directory: The directory where the file is located.
        - file_name: The name of the file to load.
        - **kwargs: Additional keyword arguments to be passed to the file reading function.

        Returns:
        - data: The loaded data from the file, or None if an error occurred.
        """
        full_path = self._get_full_path(directory, file_name)
        ext = os.path.splitext(file_name)[1].lower()

        try:
            if ext in [".csv", ".txt"]:
                data = pd.read_csv(full_path, **kwargs)
            elif ext in [".xls", ".xlsx", ".xlsm"]:
                data = pd.read_excel(full_path, engine="openpyxl", **kwargs)
            elif ext == ".json":
                with open(full_path, "r") as f:
                    data = json.load(f)
            elif ext == ".xml":
                data = ET.parse(full_path)
            elif ext == ".html":
                with open(full_path, "r", encoding="utf-8") as f:
                    data = BeautifulSoup(f, "lxml")
            else:
                logger.error(f"Unsupported file format: {ext}")
                return None

            logger.info(f"Successfully loaded file from {full_path}")
            return data

        except Exception as e:
            logger.error(f"Error loading file {full_path}: {str(e)}")
            return None

    def save_file(self, directory, file_name, data):
        """
        Save the data to a file in the specified directory with the given file name.

        Parameters:
        - directory: The directory where the file will be saved.
        - file_name: The name of the file to save.
        - data: The data to be saved.

        Returns:
        - None
        """
        full_path = self._get_full_path(directory, file_name)
        ext = os.path.splitext(file_name)[1].lower()

        try:
            if ext in [".csv", ".txt"]:
                data.to_csv(full_path, index=False)
            elif ext in [".xls", ".xlsx", ".xlsm"]:
                data.to_excel(full_path, index=False, engine="openpyxl")
            elif ext == ".json":
                with open(full_path, "w") as f:
                    json.dump(data, f, indent=4)
            elif ext == ".xml":
                data.write(full_path)
            elif ext == ".html":
                with open(full_path, "w", encoding="utf-8") as f:
                    f.write(str(data))
            else:
                logger.error(f"Unsupported file format for saving: {ext}")
                return

            logger.info(f"Successfully saved file to {full_path}")

        except Exception as e:
            logger.error(f"Error saving file {full_path}: {str(e)}")
