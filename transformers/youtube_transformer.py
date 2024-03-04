"""
This script is responsible for transforming and cleaning YouTube data obtained from various sources, 
including channel data, likes data, and subscriptions data. It also updates cached data and documents the transformations.

Key Processes:
1. Data Transformation:
   - Loads raw data for channel, likes, and subscriptions.
   - Standardises date formats in the data.
   - Populates 'thumbnail_url' based on a hierarchy of URL types.
   - Renames columns if necessary.
   - Filters and keeps only the required columns.
   - Saves the processed data in Excel format.

2. Updating Cached Data:
   - Updates the cached 'Likes & Dislikes' data with cleaned data.

3. Documentation:
   - Documents the data transformations in an Excel file.

Usage:
- Execute this script as the main module to transform and clean YouTube data.
- Ensure that the necessary data files are available in the specified directories.
- The script assumes that the data has been fetched from YouTube and needs processing.

Note:
- This script is part of a larger data processing system for managing YouTube data.
- It ensures that YouTube data is transformed, cleaned, and documented for further analysis or reporting.
"""

# Import standard libraries
import os  # For interacting with the operating system
import pandas as pd  # For data manipulation and analysis
import numpy as np  # For numerical operations
import logging  # For logging information and debugging

# Import Python system libraries
import sys  # For Python interpreter control
sys.dont_write_bytecode = True  # Prevent Python from writing .pyc files
sys.path.append('/Users/hadid/Projects/ETL')  # Add path to system path

# Import custom utility classes and functions
from utility.logging import setup_logging  # For setting up logging
from utility.file_manager import FileManager  # For file management tasks
from utility.standardise_dates import standardise_dates  # For standardising date formats
from utility.cache_data import update_cache  # For updating cached data
from utility.documentation import DataFrameDocumenter  # For documenting data frames
from utility.utils import format_data_name
from utility.utils import generate_filename

# Import constants
from constants import FileDirectory, Youtube

# Initialise logging
setup_logging()

#############################################################################################

def get_best_url(row):
    """
    Given a row from a DataFrame, find the best available URL based on a hierarchy of URL types.

    Parameters:
    - row (pandas.Series): A row from a DataFrame containing various types of URLs.

    Returns:
    - str/np.nan: The best available URL or np.nan if none found.
    """
    # Loop through URL types in a specified order to find the best available URL
    for url_type in ['maxres_url', 'standard_url', 'high_url', 'medium_url', 'default_url']:
        if pd.notna(row.get(url_type)):
            return row[url_type]
    return np.nan  # Return np.nan if no URL is found

# Loads, cleans and saves data. Returns raw df, and clean df
def process_and_save_data(file_manager, file_name, columns_needed, new_name_dict={}):
    """
    Load, process, and save data from a specified Excel file.

    Parameters:
    - file_name (str): Name of the Excel file to process.
    - columns_needed (list): List of column names that are needed in the final DataFrame.
    - new_name_dict (dict, optional): Dictionary of new column names, keys are old names, values are new names.

    Returns:
    - pandas.DataFrame, pandas.DataFrame: The original data and the processed data.
    """
    logging.info(f'Processing {file_name}')

    # Load the raw data from the specified Excel file
    data = file_manager.load_file(FileDirectory.RAW_DATA_PATH, file_name)

    # Make a copy of the raw data
    data_copy = data.copy()

    # Standardise the date format for the 'published_at' column
    # Standardize the date field if it exists
    if Youtube.DATE in data_copy.columns:
        data_updated = standardise_dates(data_copy, Youtube.DATE)
    else:
        # Log a warning if the date field is not found
        logging.warning("Date field not found in activity data.")

    # Use the get_best_url function to populate the 'thumbnail_url' column
    data_updated[Youtube.THUMBNAIL] = data_updated.apply(get_best_url, axis=1)

    # Rename columns if a renaming dictionary is provided
    if new_name_dict:
        data_updated.rename(columns=new_name_dict, inplace=True)

    # Filter the DataFrame to keep only the required columns
    data_updated = data_updated[columns_needed]

    # Save the processed data to an Excel file
    file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, data_updated, file_name)

    return data_copy, data_updated  # Return the original and processed data

#############################################################################################

def youtube_transformer():
    """
    Execute a series of data transformations on YouTube data and save it.

    This function carries out multiple steps:
    1. Process and save 'Channel' data.
    2. Process and save 'Likes' data.
    3. Process and save 'Subscriptions' data.
    4. Update cached 'Likes & Dislikes' data.
    5. Document all transformations.
    """
    # Initilaise FileManager Class
    file_manager = FileManager()

    # Columns to keep for 'Channel' data and process it
    channel_data, channel_data_updated = process_and_save_data(file_manager, Youtube.CHANNEL_DATA, Youtube.CHANNEL_FIELDS)

    # Columns to keep for 'Likes' data and process it
    likes_data, likes_data_updated = process_and_save_data(file_manager, Youtube.CACHE_LIKES_DATA, Youtube.LIKES_FIELDS, {Youtube.LEGACY_VID_ID: Youtube.VID_ID})

    # Columns to keep for 'Subscriptions' data and process it
    subs_data, subs_data_updated = process_and_save_data(file_manager, Youtube.SUBS_DATA, Youtube.SUBS_FIELDS)

    # Load raw & cleaned HTML data
    parsed_html = file_manager.load_file(FileDirectory.RAW_DATA_PATH, Youtube.PARSED_HTML_DATA)
    clean_html = file_manager.load_file(FileDirectory.CLEAN_DATA_PATH, Youtube.CLEAN_PLAYLIST_DATA)

    # Update the cached 'Likes & Dislikes' data
    updated_playlist_data = update_cache(FileDirectory.CLEAN_DATA_PATH, likes_data_updated, clean_html, Youtube.CLEAN_PLAYLIST_DATA, Youtube.VID_ID)

    # Define the path for Excel documentation
    doc_path = os.path.join(FileDirectory.DOCUMENTATION_PATH, generate_filename(Youtube.DOCUMENTATION_DATA))

    # Define the data frames to document and their corresponding names
    data_frames = {
        'raw': {format_data_name(Youtube.CHANNEL_DATA): channel_data,
                format_data_name(Youtube.CACHE_LIKES_DATA): likes_data,
                format_data_name(Youtube.PARSED_HTML_DATA): parsed_html,
                format_data_name(Youtube.SUBS_DATA): subs_data},
        'clean': {format_data_name(Youtube.CHANNEL_DATA): channel_data_updated,
                  format_data_name(Youtube.CLEAN_PLAYLIST_DATA): updated_playlist_data,
                  format_data_name(Youtube.SUBS_DATA): subs_data_updated}
    }

    # Initialise the DataFrameDocumenter class
    documenter = DataFrameDocumenter(doc_path, Youtube.SCRIPT_LOGIC)

    # Loop through each category ('raw', 'clean') and document each DataFrame
    for category, frames in data_frames.items():
        for name, df in frames.items():
            documenter.document_data_excel(df, category, name)

    # Save the documentation Excel file
    documenter.save()

if __name__ == "__main__":
    youtube_transformer()

