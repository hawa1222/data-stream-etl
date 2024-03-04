"""
This script is designed to transform Daylio data by executing a series of data preprocessing and normalisation steps.

Key Processes:
1. FileManager Initialisation:
   - Initialises FileManager for handling file operations.

2. Data Loading:
   - Loads the raw Daylio data from the specified directory.

3. Data Cleaning:
   - Cleans and preprocesses the loaded data, including date and time formatting, column selection, and logging.

4. Data Transformation to 1NF:
   - Explodes the 'activities' column into separate rows to achieve First Normal Form (1NF) compliance.

5. Data Transformation to 3NF:
   - Converts the DataFrame to Third Normal Form (3NF) by separating mood data and activities data.
   - Assigns unique IDs to activities and creates an activity list.

6. Data Saving:
   - Saves the cleaned and transformed data to separate files for further analysis.

7. Documentation:
   - Generates documentation in the form of an Excel file that includes raw, clean, and transformed data.

Usage:
- Executes as the main module to handle the transformation of Daylio data.
- Ensures that Daylio data is cleaned, normalised, and documented for analysis or reporting.

Note:
- The script assumes that the data files are in a specific format and that the required directory structure exists.
- It's part of a larger data processing system for managing and analysing Daylio data.
"""

# Import the required libraries
import os  # For operating system related functionality
import pandas as pd  # For data manipulation and analysis
import logging  # For logging information and debugging
import sys  # For Python interpreter control

# Configuration
sys.dont_write_bytecode = True  # Prevent Python from writing bytecode files (.pyc)
sys.path.append('/Users/hadid/Projects/ETL')  # Add path to system path

# Custom imports
from constants import FileDirectory, Daylio
from utility.standardise_dates import standardise_dates  # For standardising date formats
from utility.documentation import DataFrameDocumenter  # For documenting data frames
from utility.file_manager import FileManager
from utility.utils import format_data_name
from utility.logging import setup_logging
from utility.utils import generate_filename

# Setting up logging
setup_logging()

#############################################################################################

def clean_data(df):
    """
    Preprocess the DataFrame.

    Args:
    - df (DataFrame): The raw DataFrame to clean.

    Returns:
    - DataFrame: The cleaned DataFrame.
    """
    
    # Specify the format of your date and time fields
    date_format = '%Y-%m-%d'  # Date format
    time_format = '%I:%M %p'  # Time format (12-hour clock with AM/PM)
    # Combine date and time into a single string and convert to datetime
    combined_datetime = df[Daylio.DATE] + ' ' + df[Daylio.TIME].str.upper().str.replace(' ', ' ')
    df[Daylio.DATE_TIME] = pd.to_datetime(combined_datetime, format=f'{date_format} {time_format}', errors='coerce')

    
    df = standardise_dates(df, Daylio.DATE_TIME)  # Standardise the 'date_time' column to a consistent datetime format


    df = df.loc[:, Daylio.CLEAN_FIELDS]  # Select only the columns defined in DAYLIO_COLUMNS

    logging.info('Dataframe Cleaned')

    return df  # Return the cleaned DataFrame

def explode_activities(df):
    """
    Explode the activities into separate rows.

    Args:
    - df (DataFrame): The DataFrame containing activities.

    Returns:
    - DataFrame: The DataFrame with exploded activities.
    """
    # Split 'activities' into a list and apply it to create new rows for each activity
    df[Daylio.ACTIVITY] = df[Daylio.ACTIVITY].apply(lambda x: str(x).split(' | ') if pd.notna(x) else [])
    df = df.explode(Daylio.ACTIVITY, ignore_index=True)  # Explode the list into separate rows

    logging.info('Dataframe 1NF compliant')

    return df  # Return the modified DataFrame

#############################################################################################

def convert_to_3NF(df):
    """
    Convert the DataFrame to Third Normal Form (3NF).

    Args:
    - df (DataFrame): The DataFrame to normalise.

    Returns:
    - Tuple[DataFrame]: Returns a tuple containing DataFrames for moods, activities, and a list of unique activities.
    """

    # Drop duplicates based on 'date_time' and remove 'activities' column
    df_mood = df.drop_duplicates(
        subset=[Daylio.DATE_TIME]).drop(columns=[Daylio.ACTIVITY]).reset_index(drop=True)

    # Keep only 'date_time' and 'activities', drop duplicates
    df_activities = df[[Daylio.DATE_TIME, Daylio.ACTIVITY]].drop_duplicates().reset_index(drop=True)

    # Extract unique activity names from the 'activities' column and reset index
    unique_activities = df[Daylio.ACTIVITY].drop_duplicates().reset_index(drop=True)
    # Create a new DataFrame that assigns an ID to each unique activity
    activity_list = pd.DataFrame(
        {Daylio.ACTIVITY_ID: range(1, len(unique_activities) + 1),
         Daylio.ACTIVITY_NAME: unique_activities})

    # Merge df_activities with activity_list to replace 'activities' names with their corresponding IDs
    df_activities = pd.merge(
        df_activities, activity_list, how='left', left_on=Daylio.ACTIVITY,
        right_on=Daylio.ACTIVITY_NAME)

    # Remove redundant columns and add a primary key
    df_activities.drop(columns=[Daylio.ACTIVITY, Daylio.ACTIVITY_NAME], inplace=True)
    df_activities[Daylio.ID] = df_activities.index + 1

    logging.info('Dataframe split into 3NF compliant format: df_mood, df_activities, activity_list')

    return df_mood, df_activities, activity_list  # Return the 3NF DataFrames as a tuple

#############################################################################################

def daylio_transformer():
    """
    Main function to transform the Daylio data. It orchestrates the execution of helper functions
    for data loading, cleaning, transformation, and documentation.
    """

    # Initilaise FileManager Class
    file_manager = FileManager()

    # Load Data
    df = file_manager.load_file(FileDirectory.RAW_DATA_PATH, Daylio.CLEAN_DATA)  # Load the raw Daylio data

    # Data Transformations
    clean_df = clean_data(df)

    # Explode the 'activities' into separate rows and normalise the data to 3NF
    oneNF_df = explode_activities(clean_df)
    df_mood, df_activities, df_activities_list = convert_to_3NF(oneNF_df)

    # Save the cleaned and transformed data
    # Define a dictionary mapping Daylio constants to their respective dataframes
    datasets = {
        Daylio.CLEAN_DATA: clean_df,
        Daylio.MOOD_DATA: df_mood,
        Daylio.ACTIVITY_DATA: df_activities,
        Daylio.ACTIVITY_LIST_DATA: df_activities_list
    }
    
    # Use a loop to save each dataframe
    for file_name, dataframe in datasets.items():
        file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, dataframe, file_name)

    # Create a dictionary of DataFrames categorised as 'raw', 'clean', and 'transformed'
    data_frames = {
        'raw': {format_data_name(Daylio.CLEAN_DATA): df},
        'clean': {format_data_name(Daylio.CLEAN_DATA): clean_df},
        'transformed': {format_data_name(Daylio.MOOD_DATA): df_mood,
                        format_data_name(Daylio.ACTIVITY_DATA): df_activities,
                        format_data_name(Daylio.ACTIVITY_LIST_DATA): df_activities_list}
    }

    # Path for the documentation file
    doc_path = os.path.join(FileDirectory.DOCUMENTATION_PATH, generate_filename(Daylio.DOCUMEMTATION_DATA))
    # Initialise DataFrameDocumenter
    documenter = DataFrameDocumenter(doc_path, Daylio.SCRIPT_LOGIC)

    # Loop through each category and DataFrame for documentation
    for category, frames in data_frames.items():
        for name, df in frames.items():
            documenter.document_data_excel(df, category, name)  # Document each DataFrame

    # Save the documentation Excel file
    documenter.save()

# Entry point of the script
if __name__ == '__main__':
    """
    Executes the daylio_transformer function to start the data transformation pipeline.
    """
    daylio_transformer()

