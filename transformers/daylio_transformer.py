"""
This script is designed to transform Daylio data by executing a series of data preprocessing and normalisation steps.

Key Processes:
1. FileManager Initialisation:
   - Initialises FileManager for handling file operations.

2. Data Loading:
   - Loads the raw Daylio data from the specified directory.

3. Data Cleaning:
   - Cleans and preprocesses the loaded data, including date and time formatting, column selection, and logging.

4. Data Transformation:
   - Explodes the 'activities' column into separate rows to achieve First Normal Form (1NF) compliance &
   create two seperate tables for mood and activities data.

5. Data Saving:
   - Saves the cleaned and transformed data to separate files for further analysis.

6. Documentation:
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

# Custom imports
from constants import FileDirectory, Daylio
from utility.standardise_dates import standardise_dates  # For standardising date formats
from utility.documentation import DataFrameDocumenter  # For documenting data frames
from utility.file_manager import FileManager
from utility.utils import format_data_name
from utility.logging import setup_logging
from utility.utils import generate_filename

# Setting up logging
logger = setup_logging()

#############################################################################################

def clean_data(df):
    """
    Preprocess the DataFrame.

    Args:
        df (DataFrame): The raw DataFrame to clean.

    Returns:
        DataFrame: The cleaned DataFrame.
    """

    # Specify the format of your date and time fields
    datetime_format = '%Y-%m-%d %I:%M%p'  # Datetime format (e.g., '2024-04-27 8:00PM')

    # Remove the '‚ÄØ' character from the time values
    df[Daylio.TIME] = df[Daylio.TIME].str.replace('‚ÄØ', '', regex=False)

    # Combine date and time into a single string and convert to datetime
    combined_datetime = df[Daylio.DATE] + ' ' + df[Daylio.TIME].str.upper().str.replace(r'[^a-zA-Z0-9:\- ]', '', regex=True)
    # Remove any leading/trailing whitespace from the combined_datetime strings
    combined_datetime = combined_datetime.str.strip()

    # Convert the combined_datetime strings to datetime objects
    df[Daylio.DATE_TIME] = pd.to_datetime(combined_datetime, format=datetime_format, errors='coerce')
    df = standardise_dates(df, Daylio.DATE_TIME)  # Standardise the 'date_time' column to a consistent datetime format

    df = df.loc[:, Daylio.CLEAN_FIELDS]  # Select only the columns defined in DAYLIO_COLUMNS
    logger.info('Dataframe Cleaned')
    return df  # Return the cleaned DataFrame

def transform_data(df):
    """
    Create the 'activities' table from the cleaned DataFrame.

    Args:
        df (DataFrame): The cleaned DataFrame.

    Returns:
        Tuple[DataFrame]: A tuple containing the main DataFrame and the activities DataFrame.
    """
    # Create main DataFrame by dropping the 'activities' column and removing duplicates
    df_main = df.drop(columns=[Daylio.ACTIVITY]).drop_duplicates(subset=[Daylio.DATE_TIME]).reset_index(drop=True)

    # Create the activities DataFrame
    # Subset the DataFrame to include only the 'date_time' and 'activities' columns
    df_activities = df[[Daylio.DATE_TIME, Daylio.ACTIVITY]]
    # Print top 5 rows of the DataFrame
    # Split the 'activities' column into a list of activities
    df_activities.loc[:, Daylio.ACTIVITY] = df_activities[Daylio.ACTIVITY].apply(lambda x: str(x).split(' | ') if pd.notna(x) else [])
    # Explode the 'activities' column to create separate rows for each activity
    df_activities = df_activities.explode(Daylio.ACTIVITY)
    # Drop duplicates and reset the index
    df_activities = df_activities.drop_duplicates().reset_index(drop=True)
    # Create a ID field for each row
    df_activities[Daylio.ID] = df_activities.index + 1
    logger.info('Mood & Activities table created')

    return df_main, df_activities  # Return the main DataFrame and activities DataFrame as a tuple

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

    # Clean Data
    clean_df = clean_data(df)

    # Transform Data
    df_mood, df_activities = transform_data(clean_df)

    # Save the cleaned and transformed data
    # Define a dictionary mapping Daylio constants to their respective dataframes
    datasets = {
        Daylio.CLEAN_DATA: clean_df,
        Daylio.MOOD_DATA: df_mood,
        Daylio.ACTIVITY_DATA: df_activities
    }
    
    # Use a loop to save each dataframe
    for file_name, dataframe in datasets.items():
        file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, dataframe, file_name)

    # Create a dictionary of DataFrames categorised as 'raw', 'clean', and 'transformed'
    data_frames = {
        'raw': {format_data_name(Daylio.CLEAN_DATA): df},
        'clean': {format_data_name(Daylio.CLEAN_DATA): clean_df},
        'transformed': {format_data_name(Daylio.MOOD_DATA): df_mood,
                        format_data_name(Daylio.ACTIVITY_DATA): df_activities}
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

