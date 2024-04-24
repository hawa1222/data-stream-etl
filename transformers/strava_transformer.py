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

2. Partial Data Normalisation:
   - Divides data into two DataFrame tables: Activity Information and Performance Metrics.

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

# Custom imports
from constants import FileDirectory, StravaAPI
from utility.standardise_dates import standardise_dates
from utility.utils import format_data_name
from utility.file_manager import FileManager
from utility.logging import setup_logging
from utility.documentation import DataFrameDocumenter
from utility.utils import generate_filename

# Setting up logging
logger = setup_logging()

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
        StravaAPI.LEGACY_ACT_ID: StravaAPI.ACTIVITY_ID,
        StravaAPI.LEGACY_ACT_NAME: StravaAPI.ACTIVITY_NAME,
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
    
    logger.info('Cleaned & transformed data')

    return df

def split_data(df: pd.DataFrame):
    """
    Normalises the data for 3rd normal form (3NF) compliance.

    Parameters:
        df: Input DataFrame.

    Returns:
        Two DataFrames, one containing activity information with sport type and gear, and the other containing performance metrics.
    """

    # Create a DataFrame for activity information with sport type and gear
    df_activity = df[StravaAPI.ACTIVITY_FIELDS]
    # Create a DataFrame for performance metrics
    df_performance_metrics = df[StravaAPI.PERFORMANCE_FIELDS]
    logger.info("Data partially normalised into two tables: df_activity, df_performance_metrics")

    return df_activity, df_performance_metrics

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

    # Normalise data
    df_activity, df_performance_metrics = split_data(clean_strava_data)

    # Save the normalised DataFrames as Excel files
    # Define a dictionary mapping StravaAPI constants to their respective dataframes
    datasets = {
        StravaAPI.FINAL_DATA: clean_strava_data,
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
        'transformed': {format_data_name(StravaAPI.ACTIVITY_DATA): df_activity,
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



