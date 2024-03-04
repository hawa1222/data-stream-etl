# Import required libraries
import os  # For operating system-dependent functionality
import pandas as pd  # For data manipulation and analysis
import json  # For reading and writing JSON files
import chardet  # For character encoding detection
import logging  # For logging information and debugging
import xml.etree.ElementTree as ET  # For XML tree traversal
from bs4 import BeautifulSoup  # For parsing HTML and XML documents
import sys  # For Python interpreter control

# Configuration
sys.dont_write_bytecode = True  # Prevent Python from writing bytecode files (.pyc)
sys.path.append('/Users/hadid/Projects/ETL')  # Add path to system path

# Custom imports
from utility.logging import setup_logging  # Custom logging setup

# Call the logging setup function to initialise logging
setup_logging()

#############################################################################################

# Class for handling CSV files
class CSVHandler:
    """
    Class to handle operations related to CSV files.
    """

    # Load CSV file
    def load(self, file_path, **kwargs):
        """
        Load a CSV file and return it as a Pandas DataFrame.
        Uses chardet to detect file encoding.

        Parameters:
        - file_path (str): The path to the CSV file
        - **kwargs: Additional keyword arguments (not used here)

        Returns:
        - DataFrame: The loaded data
        """
        # Detect file encoding
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read())
        detected_encoding = result['encoding']

        # Try reading the file with detected encoding, fallback to utf-8
        try:
            return pd.read_csv(file_path, encoding=detected_encoding)
        except:
            return pd.read_csv(file_path, encoding='utf-8')

    # Save DataFrame to CSV
    def save(self, data, file_path):
        """
        Save a Pandas DataFrame to a CSV file.

        Parameters:
        - data (DataFrame): The data to save
        - file_path (str): The path to the CSV file where data will be saved

        Returns:
        - None
        """
        data.to_csv(file_path, index=False)

# Class for handling Excel files
class ExcelHandler:
    """
    Class to handle operations related to Excel files.
    """

    # Load Excel file
    def load(self, file_path, **kwargs):
        """
        Load an Excel file and return it as a Pandas DataFrame.

        Parameters:
        - file_path (str): The path to the Excel file
        - **kwargs: Additional keyword arguments (like sheet_name)

        Returns:
        - DataFrame: The loaded data
        """
        sheet_name = kwargs.get('sheet_name', 0)  # Get sheet_name, default is 0
        return pd.read_excel(file_path,  engine='openpyxl', sheet_name=sheet_name)

    # Save DataFrame to Excel
    def save(self, data, file_path):
        """
        Save a Pandas DataFrame to an Excel file.

        Parameters:
        - data (DataFrame): The data to save
        - file_path (str): The path to the Excel file where data will be saved

        Returns:
        - None
        """
        data.to_excel(file_path, index=False)

# Class for handling XML files
class XMLHandler:
    """
    Class to handle operations related to XML files.
    """

    # Load XML file
    def load(self, file_path):
        """
        Load an XML file and return it as an ElementTree object.

        Parameters:
        - file_path (str): The path to the XML file

        Returns:
        - ElementTree: The loaded XML tree
        """
        try:
            tree = ET.parse(file_path)  # Parse XML
        except ET.ParseError:  # Handle parsing errors
            logging.info("Failed to parse the XML file.")
        return tree

    # Save ElementTree to XML
    def save(self, data, file_path):
        """
        Save an ElementTree object to an XML file.

        Parameters:
        - data (ElementTree): The XML tree to save
        - file_path (str): The path to the XML file where data will be saved

        Returns:
        - None
        """
        data.write(file_path)  # Write XML to file

# Class for handling JSON files
class JSONHandler:
    """
    Class to handle operations related to JSON files.
    """

    # Load JSON file
    def load(self, file_path, **kwargs):
        """
        Load a JSON file and return it as a Python object.

        Parameters:
        - file_path (str): The path to the JSON file
        - **kwargs: Additional keyword arguments (not used here)

        Returns:
        - dict/list: The loaded JSON data as a Python object
        """
        with open(file_path, 'r') as f:
            return json.load(f)  # Read and load JSON into a Python object

    # Save Python object to JSON
    def save(self, data, file_path):
        """
        Save a Python object to a JSON file.

        Parameters:
        - data (dict/list): The Python object to save
        - file_path (str): The path to the JSON file where data will be saved

        Returns:
        - None
        """
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)  # Write Python object to JSON with indentation

# Class for handling HTML files
class HTMLHandler:
    """
    Class to handle operations related to HTML files.
    """

    # Load HTML file
    def load(self, file_path):
        """
        Load an HTML file and return it as a BeautifulSoup object.

        Parameters:
        - file_path (str): The path to the HTML file

        Returns:
        - BeautifulSoup: The parsed HTML
        """
        with open(file_path, 'r', encoding='utf-8') as file:  # Open file
            soup = BeautifulSoup(file, 'lxml')  # Parse HTML
        return soup

    # Save BeautifulSoup to HTML
    def save(self, data, file_path):
        """
        Save a BeautifulSoup object to an HTML file.

        Parameters:
        - data (BeautifulSoup): The HTML to save
        - file_path (str): The path to the HTML file where data will be saved

        Returns:
        - None
        """
        html_str = str(data)  # Convert BeautifulSoup object to string
        with open(file_path, 'w', encoding='utf-8') as file:  # Open file
            file.write(html_str)  # Write HTML string to file


# Main FileManager class to handle different file types
class FileManager:
    """
    FileManager class responsible for loading and saving files.

    Attributes:
    directory (str): The directory where files are stored.

    Methods:
    load_file(file_name, **kwargs): Loads a file from the directory.
    save_file(data, file_name): Saves data to a file in the directory.
    _get_full_path(file_name): Gets the full path of a file in the directory.
    _get_handler(file_extension): Returns the appropriate file handler based on file extension.
    """

    def __init__(self):
        """
        Constructor for FileManager.

        """
        pass

    def _get_full_path(self, directory, file_name):
        """
        Get the full path for a file.

        Parameters:
        directory (str): The directory from which to load the file.
        file_name (str): Name of the file.

        Returns:
        str: Full path of the file.
        """
        return os.path.join(directory, file_name)

    def load_file(self, directory, file_name, **kwargs):
        """
        Load a file from the directory.

        Parameters:
        directory (str): The directory from which to load the file.
        file_name (str): The name of the file to load.
        **kwargs: Additional keyword arguments for file loading.

        Returns:
        Various: The loaded data from the file.
        """
        # Extract the extension from the file name
        file_extension = os.path.splitext(file_name)[1]

        # Get the appropriate handler for the file extension
        handler = self._get_handler(file_extension)

        # Check if the handler exists for the given file extension
        if handler is None:
            logging.error(f"Unsupported file format: {file_extension}")
            return None

        # Get the full path of the file
        full_path = self._get_full_path(directory, file_name)

        # Use the handler to load the data
        loaded_data = handler.load(full_path, **kwargs)

        # Log the successful file load
        logging.info(f"Successfully loaded file from {full_path}")

        return loaded_data

    def save_file(self, directory, data, file_name):
        """
        Save data to a file in the directory.

        Parameters:
        directory (str): The directory from which to load the file.
        data (Various): The data to save.
        file_name (str): The name of the file to save the data in.

        Returns:
        None
        """
        # Extract the extension from the file name
        file_extension = os.path.splitext(file_name)[1]

        # Get the appropriate handler for the file extension
        handler = self._get_handler(file_extension)

        # Check if the handler exists for the given file extension
        if handler is None:
            logging.error(f"Unsupported file format: {file_extension}")
            return

        # Get the full path of the file
        full_path = self._get_full_path(directory, file_name)

        # Use the handler to save the data
        handler.save(data, full_path)

        # Log the successful file save
        logging.info(f"Successfully saved file to {full_path}")

    def _get_handler(self, file_extension):
        """
        Get the appropriate file handler based on file extension.

        Parameters:
        file_extension (str): The file extension.

        Returns:
        Various: The appropriate file handler object.
        """
        # Define a dictionary to map file extensions to their respective handlers
        handlers = {
            '.csv': CSVHandler(),
            '.xls': ExcelHandler(),
            '.xlsx': ExcelHandler(),
            '.xlsm': ExcelHandler(),
            '.xml': XMLHandler(),
            '.json': JSONHandler(),
            '.html': HTMLHandler()
        }

        # Return the handler associated with the file extension
        return handlers.get(file_extension)
