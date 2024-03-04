"""
This script transforms Strava data by selecting specified columns and performing calculations. 
It also normalises the data for 3rd normal form (3NF) compliance.

Key Processes:
1. Data Transformation:
   - Standardises dates in the DataFrame.
   - Renames multiple columns for clarity.
   - Selects only the columns of interest.
   - Converts sport text and adjusts capitalisation.
   - Converts times from seconds to minutes.

2. Data Normalisation (3NF):
   - Creates DataFrames for unique sport types and gear names.
   - Merges the original DataFrame with the new DataFrames.
   - Divides data into four DataFrames for 3NF compliance:
     - Sport Types, Gear Names, Activity Information, Performance Metrics.

3. File Management:
   - Handles file loading and saving.
   - Saves the normalised DataFrames as Excel files.

Usage:
- Execute this script as the main module to transform and normalise Strava data.
- Ensure that the required data files are available in the specified directory.

Note:
- This script is part of a larger data processing system for managing Strava data.
- Normalised data is saved as separate Excel files.
- The documentation for the script's logic is also generated and saved.
"""


# Import the required libraries
import os
import pandas as pd
import logging
import sys

# Configuration
sys.dont_write_bytecode = True  # Prevent Python from writing bytecode files (.pyc)
sys.path.append('/Users/hadid/Projects/ETL')  # Add path to system path

# Custom imports
from constants import FileDirectory, StravaAPI
from utility.standardise_dates import standardise_dates
from utility.utils import format_data_name
from utility.file_manager import FileManager
from utility.logging import setup_logging
from utility.documentation import DataFrameDocumenter
from utility.utils import generate_filename

# Setting up logging
setup_logging()

#############################################################################################

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the data by selecting specified columns and performing calculations.

    Parameters:
        df: Input DataFrame containing the raw data.

    Returns:
        DataFrame after applying the transformations.
    """

    df = standardise_dates(df, StravaAPI.DATE)

    # Renaming multiple columns in the DataFrame
    df = df.rename(columns={
        StravaAPI.LEGACY_GEAR: StravaAPI.GEAR_NAME,
        StravaAPI.ID: StravaAPI.ACTIVITY_ID
    })

    # Select only the columns we're interested in
    df = df[StravaAPI.CLEAN_FIELDS]

    # Create a copy of the DataFrame to avoid warnings
    df = df.copy()

    # Replace the old sport text with the new one
    df[StravaAPI.SPORT] = df[StravaAPI.SPORT].replace(StravaAPI.SPORT_TEXT, StravaAPI.SPORT_TEXT_NEW)
    # Add a space before each capital letter that follows a lowercase letter
    df[StravaAPI.SPORT] = df[StravaAPI.SPORT].str.replace(r'(?<=[a-s])(?=[A-Z])', ' ', regex=True)

    # Convert times from seconds to minutes
    df[StravaAPI.MOVE_TIME] = (df[StravaAPI.MOVE_TIME] / 60).round(2)
    df[StravaAPI.ELAP_TIME] = (df[StravaAPI.ELAP_TIME] / 60).round(2)
    
    

    logging.info('Transformed data')

    return df

def normalise_data_for_3nf(df: pd.DataFrame):
    """
    Normalises the data for 3rd normal form (3NF) compliance.

    Parameters:
        df: Input DataFrame.

    Returns:
        Four DataFrames each containing a subset of the data for 3NF compliance.
    """

    # Create a DataFrame with unique sport types
    df_sport_type = df[[StravaAPI.SPORT]].drop_duplicates().reset_index(drop=True)
    df_sport_type[StravaAPI.SPORT_ID] = range(1, len(df_sport_type) + 1)

    # Create a DataFrame with unique gear names, dropping any null values
    df_gear = df[[StravaAPI.GEAR_NAME]].drop_duplicates().dropna().reset_index(drop=True)
    df_gear[StravaAPI.GEAR_ID] = range(1, len(df_gear) + 1)

    # Merge the original DataFrame with the newly created DataFrames
    df = pd.merge(df, df_sport_type, how='left', on=StravaAPI.SPORT)
    df = pd.merge(df, df_gear, how='left', left_on=StravaAPI.GEAR_NAME, right_on=StravaAPI.GEAR_NAME)

    # Create a DataFrame for activity information
    df_activity = df[StravaAPI.ACTIVITY_FIELDS]

    # Create a DataFrame for performance metrics
    df_performance_metrics = df[StravaAPI.PERFORMANCE_FIELDS]

    logging.info("Data normalised into 3NF compliant tables: df_sport_type, df_gear, df_activity, df_performance_metrics")

    return df_sport_type, df_gear, df_activity, df_performance_metrics

#############################################################################################

def strava_transformer():
    """
    Main function that orchestrates data transformation for Strava data.
    """

    # Initilaise FileManager Class
    file_manager = FileManager()

    # Load raw data
    strava_data = file_manager.load_file(FileDirectory.RAW_DATA_PATH, StravaAPI.FINAL_DATA)

    # Transform the data
    clean_strava_data = transform_data(strava_data)

    # Normalise data for 3NF compliance
    df_sport_type, df_gear, df_activity, df_performance_metrics = normalise_data_for_3nf(clean_strava_data)

    # Save the normalised DataFrames as Excel files
    # Define a dictionary mapping StravaAPI constants to their respective dataframes
    datasets = {
        StravaAPI.FINAL_DATA: clean_strava_data,
        StravaAPI.SPORT_DATA: df_sport_type,
        StravaAPI.GEAR_DATA: df_gear,
        StravaAPI.ACTIVITY_DATA: df_activity,
        StravaAPI.PERFORMANCE_DATA: df_performance_metrics
    }
    
    # Use a loop to save each dataframe as an Excel file
    for file_name, dataframe in datasets.items():
        file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, dataframe, file_name)


    # Define data frames and their corresponding names
    data_frames = {
        'raw': {format_data_name(StravaAPI.FINAL_DATA): strava_data},
        'clean': {format_data_name(StravaAPI.FINAL_DATA): clean_strava_data},
        'transformed': {format_data_name(StravaAPI.SPORT_DATA): df_sport_type,
                       format_data_name(StravaAPI.GEAR_DATA): df_gear,
                       format_data_name(StravaAPI.ACTIVITY_DATA): df_activity,
                       format_data_name(StravaAPI.PERFORMANCE_DATA): df_performance_metrics}
    }

    # Define the documentation file path
    doc_path = os.path.join(FileDirectory.DOCUMENTATION_PATH, generate_filename(StravaAPI.DOCUMEMTATION_DATA))
    # Initialize the DataFrameDocumenter class
    documenter = DataFrameDocumenter(doc_path, StravaAPI.SCRIPT_LOGIC)

    # Loop through each category ('raw', 'clean') and each DataFrame
    for category, frames in data_frames.items():
        for name, df in frames.items():
            documenter.document_data_excel(df, category, name)

    # Save the Excel file
    documenter.save()

# Execute the main function
if __name__ == "__main__":
    strava_transformer()



