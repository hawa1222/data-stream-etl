# Import the required libraries
"""
This script manages the transformation and documentation of financial data, specifically for 'Spend' data.

Key Processes:
1. Data Cleaning:
   - The `clean_data` function is called to remove empty rows, drop the first row, and standardise the date format in the DataFrame.
   - Ensures the data is clean and consistent, ready for further processing or analysis.

2. FileManager Initialisation and Data Loading:
   - Initialises FileManager for handling file operations.
   - Loads raw financial data from a specified directory.

3. Applying Data Cleaning:
   - Calls the `clean_data` function to clean the loaded DataFrame.

4. Data Saving:
   - Saves the cleaned and normalised DataFrame back to a specified directory as Excel files.

5. Data Documentation:
   - Creates a dictionary to hold both raw and cleaned DataFrames for documentation purposes.
   - Initialises DataFrameDocumenter to document the data transformation process.
   - Loops through each category (raw, clean) and documents each DataFrame in an Excel file.
   - Saves the documented Excel file to a specified documentation path.

Usage:
- Run as the main module to execute the data transformation pipeline for 'Spend' data.
- Facilitates the cleaning, transformation, and documentation of financial data for subsequent use in data analysis or reporting.

Note:
- The script focuses on ensuring data integrity and consistency through a series of cleaning and standardisation steps.
- It is a critical component of a broader data processing and analysis workflow.
"""


# Import the required libraries
import os  # For operating system related functionality
import logging  # For logging information and debugging
import sys  # For Python interpreter control
import pandas as pd

# Configuration
sys.dont_write_bytecode = True  # Prevent Python from writing bytecode files (.pyc)
sys.path.append('/Users/hadid/Projects/ETL')  # Add path to system path

# Custom imports
from constants import FileDirectory, Spend
from config import Settings
from utility.utils import format_data_name
from utility.documentation import DataFrameDocumenter  # For documenting data frames
from utility.file_manager import FileManager
from utility.logging import setup_logging
from utility.utils import generate_filename

# Setting up logging
setup_logging()

#############################################################################################

# Remove all empty rows and standardise data
def clean_data(df):
    """
    Cleans up the DataFrame by first removing the first row, then removing empty rows,
    and formatting the date column.

    Parameters:
    df (pd.DataFrame): Original DataFrame that needs to be cleaned.

    Returns:
    pd.DataFrame: Cleaned and processed DataFrame with a new id column and formatted date.
    """

    # Drop the first row of the DataFrame
    df = df.drop(df.index[0]).reset_index(drop=True)

    # Drop rows where all values except in 'period' & 'transaction_id' are NaN
    n = len(df.columns)
    new_df = df[df.isnull().sum(axis=1) < n-2].reset_index(drop=True)

    # Check if 'date' column exists and convert its format
    if Spend.DATE in new_df.columns:
        # Convert 'date' column to datetime format
        new_df[Spend.DATE] = pd.to_datetime(new_df[Spend.DATE]).dt.strftime(Settings.DATE_FORMAT)

    logging.info("Cleaned Spend data")

    return new_df

#############################################################################################

# Main function to execute data transformation pipeline
def spend_transformer():
    """
    The main function that carries out the sequence of data transformation tasks.
    """
    # Initilaise FileManager Class
    file_manager = FileManager()

    # Load raw data
    df = file_manager.load_file(FileDirectory.RAW_DATA_PATH, Spend.CLEAN_DATA)

    # Clean the loaded data
    clean_df = clean_data(df)

    # Save cleaned and normalised DataFrames as Excel files
    file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, clean_df, Spend.CLEAN_DATA)
   
    # Create dictionary to hold DataFrames for documentation
    data_frames = {
        'raw': {format_data_name(Spend.CLEAN_DATA): df},
        'clean': {format_data_name(Spend.CLEAN_DATA): clean_df}
    }

    # Path for the documentation file
    doc_path = os.path.join(FileDirectory.DOCUMENTATION_PATH, generate_filename(Spend.DOCUMEMTATION_DATA))
    # Initialise documentation object
    documenter = DataFrameDocumenter(doc_path, Spend.SCRIPT_LOGIC)
    # Loop through each category and DataFrame for documentation
    for category, frames in data_frames.items():
        for name, df in frames.items():
            documenter.document_data_excel(df, category, name)

    # Save documentation
    documenter.save()

    return clean_df

if __name__ == '__main__':
    spend_transformer()
