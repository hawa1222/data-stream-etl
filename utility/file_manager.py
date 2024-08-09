"""
Script to load and save files in different formats, and update Excel files with new data.
"""

import json
import os
import warnings

from xml.etree import ElementTree

import pandas as pd

from bs4 import BeautifulSoup

from utility.log_manager import setup_logging

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

    def load_file(self, directory, file_name, extension="xlsx", **kwargs):
        """
        Load a file from the specified directory with the given file name.

        Parameters:
            directory: The directory where the file is located.
            file_name: The name of the file to load.
            **kwargs: Additional keyword arguments to be passed to the file reading function.

        Returns:
            data: The loaded data from the file, or None if an error occurred.
        """
        full_path = self._get_full_path(directory, file_name) + "." + extension

        try:
            if extension in ["csv", "txt"]:
                data = pd.read_csv(full_path, **kwargs)
            elif extension in ["xls", "xlsx", "xlsm"]:
                data = pd.read_excel(full_path, engine="openpyxl", **kwargs)
            elif extension == "json":
                with open(full_path) as f:
                    data = json.load(f)
            elif extension == "xml":
                data = ElementTree.parse(full_path)
            elif extension == "html":
                with open(full_path, encoding="utf-8") as f:
                    data = BeautifulSoup(f, "lxml")
            else:
                logger.error(f"Unsupported file format: {extension}")
                return None

            logger.info(f"Successfully loaded file from {full_path}")
            return data

        except Exception as e:
            logger.error(f"Error loading file {full_path}: {str(e)}")
            raise

    def save_file(self, directory, file_name, data, extension="xlsx"):
        """
        Save the data to a file in the specified directory with the given file name.

        Parameters:
            directory: The directory where the file will be saved.
            file_name: The name of the file to save.
            data: The data to be saved.
            extension: The file extension to use for saving the file (default: "xlsx").

        Returns:
            None
        """
        full_path = self._get_full_path(directory, file_name) + "." + extension

        try:
            if extension in ["xls", "xlsx", "xlsm"]:
                data.to_excel(full_path, index=False, engine="openpyxl")
            elif extension in ["csv", "txt"]:
                data.to_csv(full_path, index=False)
            elif extension == "json":
                with open(full_path, "w") as f:
                    json.dump(data, f, indent=4)
            elif extension == "xml":
                data.write(full_path)
            elif extension == "html":
                with open(full_path, "w", encoding="utf-8") as f:
                    f.write(str(data))
            else:
                logger.error(f"Unsupported file format for saving: {extension}")
                return

            logger.info(f"Successfully saved file to {full_path}")

        except Exception as e:
            logger.error(f"Error saving file {full_path}: {str(e)}")
            raise


def update_excel(file_directory, file_name, new_data):
    """
    Update an existing Excel file with new data.

    Parameters:
        file_directory: The directory where the file is located.
        file_name: The name of the file to update.
        new_data: The new data to be added to the existing file.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    logger.debug(f"Updating local copy of '{file_name}'...")

    if new_data.empty:
        logger.warning("New dataframe is empty. Skipping upload to local disk")
        return False

    local_data_path = os.path.join(file_directory, f"{file_name}.xlsx")

    file_manager = FileManager()

    try:
        if os.path.exists(local_data_path):
            existing_data = file_manager.load_file(file_directory, file_name)
            logger.debug(
                f"Local copy loaded. Existing data shape: {existing_data.shape}, "
                f"New data shape: {new_data.shape}"
            )
        else:
            existing_data = pd.DataFrame()
            logger.debug("Local copy doesn't exist, empty dataframe created")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)
            combined_data = pd.concat([existing_data, new_data], join="outer", ignore_index=True)
            logger.info(f"Local copy updated. Updated data shape: {combined_data.shape}")

        file_manager.save_file(file_directory, file_name, combined_data)

        return True

    except Exception as e:
        logger.error(f"Error updating Excel file '{local_data_path}': {str(e)}")
        raise
