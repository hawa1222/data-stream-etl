# Import the required libraries
import os  # For operating system related functionality
import pandas as pd  # For data manipulation and analysis
import logging  # For logging information and debugging
import textwrap  # For text wrapping and filling
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from collections import defaultdict
import pytz
from copy import deepcopy
from pandas import DataFrame
from typing import Dict
import sys  # For Python interpreter control

# Configuration
sys.dont_write_bytecode = True  # Prevent Python from writing bytecode files (.pyc)
sys.path.append('/Users/hadid/Projects/ETL')  # Add path to system path

# Custom imports
from constants import FileDirectory, AppleHealth
from utility.standardise_dates import standardise_dates  # For standardising date formats
from utility.standardise_fields import DataStandardiser  # For standardising data fields
from utility.documentation import DataFrameDocumenter  # For documenting data frames
from utility.file_manager import FileManager
from utility.logging import setup_logging

# Initialize logging
setup_logging()

##################################################################################################################################
def strip_list_elements(input_list):
    """
    Strips each element in the list of leading and trailing whitespaces and replaces any special characters.

    Parameters:
        input_list (list): A list of elements to be stripped and cleaned.

    Returns:
        list: A list where each element is stripped and special characters are replaced.
    """
    return [element.strip().replace('\ufeff', '') for element in input_list]

def create_data_source_dict(apple_instruc_data, record_names):
    """
    Create a dictionary of data sources based on the given DataFrame and list of record names.

    The resulting dictionary has the record name as the key and another dictionary as the value.
    The inner dictionary contains information like 'sources', 'timeframe', 'agg_type', 'fields', and 'group'.

    Parameters:
        apple_instruc_data (pd.DataFrame): DataFrame containing the configuration settings.
        record_names (list): List of DataFrame names that need to be included in the dictionary.

    Returns:
        dict: A dictionary containing data source information for each DataFrame name.
    """

    # Initializing an empty dictionary to store the data source information.
    data_source_dict = {}

    # Looping through each DataFrame name specified in record_names
    for df_name in record_names:

        # Filtering the DataFrame based on the current DataFrame name
        filtered_row = apple_instruc_data.query(f"dataframe_name == '{df_name}'")

        # Check if the DataFrame contains any information for the given DataFrame name.
        if not filtered_row.empty:

            # Extracting information from the DataFrame and storing it in the dictionary.
            # strip_list_elements function is used to clean the data.
            data_source_dict[df_name.strip()] = {
                'sources': strip_list_elements(str(filtered_row['data_source'].iloc[0]).split(', ')),
                'timeframe': strip_list_elements(str(filtered_row['timeframe'].iloc[0]).split(', ')),
                'agg_type': strip_list_elements(str(filtered_row['aggregation_type'].iloc[0]).split(', ')),
                'fields': strip_list_elements(str(filtered_row['fields'].iloc[0]).split(', ')),
                'group': strip_list_elements(str(filtered_row['group'].iloc[0]).split(', '))
            }

    return data_source_dict

##################################################################################################################################
# Function to Process 'ActivitySummary' in the DataFrame Dictionary
def process_activity_summary(dataframes_dict):
    """
    Processes the 'ActivitySummary' DataFrame in the dataframes_dict.
    Keeps only specific columns and updates the dictionary.

    Parameters:
        dataframes_dict (dict): A dictionary containing DataFrames.
    """
    # Check if 'ActivitySummary' exists in the dictionary
    if 'activity_summary' not in dataframes_dict:
        logging.debug("The key ActivitySummary does not exist in the provided dictionary.")
        return

    # Log that we are starting to transform 'ActivitySummary'
    logging.info("Transforming ActivitySummary")

    # Create a copy of the 'ActivitySummary' DataFrame
    df_copy = dataframes_dict['activity_summary'].copy()

    # Define the columns we want to keep
    columns_to_keep = ['date_components', 'active_energy_burned', 'apple_exercise_time', 'apple_stand_hours']

    # Keep only the specified columns
    df_copy = df_copy[columns_to_keep]

    # Convert specific columns to numeric data types, handling errors by coercing them to NaN
    for col in ['active_energy_burned', 'apple_exercise_time', 'apple_stand_hours']:
        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')

    # Rename 'dateComponents' to 'date'
    df_copy.rename(columns={'date_components': 'date'}, inplace=True)

    # Update the original dictionary with the transformed DataFrame
    dataframes_dict['activity_summary'] = df_copy

# Process records and transform them
def process_record(df_dict, apple_data):
    """
    Transforms and filters the 'Record' DataFrame in the provided dictionary based on apple_data information.

    Parameters:
        df_dict (dict): A dictionary containing DataFrames.
        apple_data (pd.DataFrame): DataFrame containing information about which records to keep and transform.

    Returns:
        dict: A dictionary containing filtered and transformed DataFrames.
    """

    def transform_df(df):
        """
        Transforms the DataFrame with various operations like timezone conversion, data extraction, and so on.

        Parameters:
            df (pd.DataFrame): The DataFrame to transform.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        # Make a copy of the DataFrame to prevent modifying the original
        df = df.copy()

        # Columns that need to be converted to datetime
        date_cols = ['creation_date', 'start_date', 'end_date']

        # Convert these columns to datetime and localize the timezone to London time
        london_tz = pytz.timezone('Europe/London')
        df[date_cols] = df[date_cols].apply(pd.to_datetime).apply(lambda x: x.dt.tz_convert(london_tz))

        # Remove timezone info for simplification
        df[date_cols] = df[date_cols].apply(lambda x: x.dt.tz_localize(None))

        # Extract only the date part from 'startDate'
        df['date'] = df['start_date'].dt.strftime('%Y-%m-%d')

        # Extract the hour from 'startDate'
        df['hour'] = df['start_date'].dt.hour

        # Store the original 'value' into a new column
        df['original_value'] = df['value']

        # Convert 'value' to numeric, handling errors by coercing them to NaN and then filling NaNs with 1.0
        df['value'] = pd.to_numeric(df['value'], errors='coerce').fillna(1.0)

        # Simplify the 'type' by removing 'Identifier'
        df['type'] = df['type'].str.split('Identifier').str[-1]

        # Remove leading and trailing spaces from 'sourceName'
        df['source_name'] = df['source_name'].str.strip()

        return df

    # Transform the DataFrame for the 'Record' key
    transformed_df = transform_df(df_dict['record'])

    # Filter apple_data to get elements of interest
    elements_of_interest = apple_data.query("element == 'Record' and transformation == 'Y'")['dataframe_name'].tolist()

    # Create a set for quick look-up
    unique_types = set(transformed_df['type'].unique())

    # Initialize the dictionary to hold the preview tables
    preview_tables = {}

    # Loop over each element of interest
    for elem in elements_of_interest:
        if elem in unique_types:
            # Create a variable name by replacing invalid characters
            var_name = elem.replace('-', '_').replace(' ', '_')

            # Filter rows by 'type'
            filtered_df = transformed_df[transformed_df['type'] == elem]

            # Drop duplicates based on 'startDate' and 'endDate'
            preview_tables[var_name] = filtered_df.drop_duplicates(subset=['start_date', 'end_date'])
        else:
            logging.info(f"The specified type '{elem}' does not exist in the DataFrame.")

    return preview_tables

##################################################################################################################################
# Transform records
def handle_sleep_analysis(df):
    """
    Transforms the SleepAnalysis DataFrame.
    """
    # Extract the date from creationDate
    df['date'] = df['creation_date'].dt.date
    # Remove duplicate rows based on the date
    df.drop_duplicates(subset=['date'], inplace=True)
    # Calculate time in bed, restless time and convert time to hours
    df['time_in_bed'] = (df['awake_time'] - df['bed_time']).dt.total_seconds() / 3600

    # Convert 'time_in_bed' and 'total_time_asleep' to timedelta if they aren't already
    df['time_in_bed'] = pd.to_timedelta(df['time_in_bed'])
    df['total_time_asleep'] = pd.to_timedelta(df['total_time_asleep'])
    # Perform subtraction operation. The result will be a timedelta64[ns] type
    time_diff = df['time_in_bed'] - df['total_time_asleep']
    # Convert the resulting timedelta to total seconds
    time_diff_seconds = time_diff.dt.total_seconds()
    # Now divide these total seconds by 3600 to convert to hours
    df['restless_time'] = time_diff_seconds / 3600

    df['total_time_asleep'] = df['total_time_asleep'].dt.total_seconds() / 3600

    return df

def transform_record_dicts(record_dict: Dict[str, DataFrame], apple_data: DataFrame) -> Dict[str, DataFrame]:
    """
    Transforms record dictionaries based on instructions in apple_data DataFrame.
    """
    # Get DataFrame names for which transformation should be applied
    record_Names = apple_data.query("`element` == 'Record' and `transformation` == 'Y'")['dataframe_name'].tolist()

    # Create a dictionary containing data source information for each DataFrame
    data_source_dict = create_data_source_dict(apple_data, record_Names)

    # Initialize an empty dictionary to hold new DataFrames
    new_dataframe_dict = {}

    for df_name in record_Names:
        # Get the original DataFrame
        original_df = record_dict.get(df_name)

        # Skip if DataFrame does not exist
        if original_df is None:
            continue

        # Create a copy to prevent modifying the original DataFrame
        new_df = original_df.copy()

        # Extract source-related information
        source_info = data_source_dict.get(df_name, {})
        source_names = source_info.get('sources')
        agg_type = source_info.get('agg_type')[0].capitalize()
        fields = source_info.get('fields')
        timeframes = source_info.get('timeframe')

        # Initialize an empty dictionary for aggregation
        agg_dict = {}

        logging.info(f'Transforming {df_name}')

        # Filter DataFrame by source names if specified
        if source_names:
            new_df = new_df.query("source_name in @source_names").copy()

        # Apply transformations specific to each DataFrame type
        if df_name == 'MindfulSession':
            new_df.loc[:, 'duration_min'] = (new_df['end_date'] - new_df['start_date']).dt.total_seconds() / 60
            agg_dict.update(dict(zip(fields, [('duration_min', 'sum'), ('value', 'count')])))
            new_df = new_df.groupby('date').agg(**agg_dict).reset_index()

        elif df_name == 'SleepAnalysis':

            new_df = new_df.query("original_value == 'HKCategoryValueSleepAnalysisInBed'").copy()

            new_df.loc[:, 'time_asleep'] = new_df['end_date'] - new_df['start_date']
            agg_dict.update(dict(zip(fields, [
                ('time_asleep', 'sum'),
                ('start_date', 'min'),
                ('end_date', 'max'),
                ('creation_date', 'count'),
                ('time_asleep', lambda x: (x // timedelta(minutes=90)).sum())
            ])))
            new_df = new_df.groupby('creation_date').agg(**agg_dict).reset_index()
            new_df = handle_sleep_analysis(new_df)

        else:
            agg_dict = {field: ('value', agg_type.lower()) for field in fields}
            new_df = new_df.groupby(timeframes).agg(**agg_dict).reset_index()

        # Store the transformed DataFrame
        new_dataframe_dict[df_name] = new_df

    return new_dataframe_dict

##################################################################################################################################
# Joins data by group
def join_data_by_group(complete_dict, apple_data):
    """
    Join multiple DataFrames by group based on a configuration DataFrame.

    Parameters:
        complete_dict (dict): Dictionary containing DataFrames to join.
        apple_data (pd.DataFrame): DataFrame containing configuration info.

    Returns:
        dict: Dictionary containing joined DataFrames.
    """
    # Querying DataFrame names of interest from 'apple_data' DataFrame
    elements_of_interest = apple_data.query("`transformation` == 'Y'")['dataframe_name'].tolist()

    # Create dictionary of data sources based on elements_of_interest
    all_df_dict = create_data_source_dict(apple_data, elements_of_interest)

    # Create a copy of the complete_dict to avoid modifying the original
    complete_dict_copy = {key: df.copy() for key, df in complete_dict.items()}

    # Initialize empty dictionary to store joined data
    joined_data_by_group = {}

    for key in complete_dict_copy.keys():
        # Deep copy of DataFrame in the complete_dict
        df_a = complete_dict_copy[key]

        # Round all numerical columns to 2 decimal places, leave 0 and NaN unchanged
        for col in df_a.select_dtypes(include=['float64']).columns:
            df_a[col] = df_a[col].apply(lambda x: round(x, 2) if not pd.isna(x) and x != 0 else x)

        # If DataFrame is empty or key is not in configuration, skip to next iteration
        if df_a.empty or key not in all_df_dict:
            logging.info(f"Skipping key {key} because it is empty or not in the configuration.")
            continue

        # Fetch group name from all_df_dict
        group = all_df_dict[key]['group'][0]

        # Initialize group key-value if it doesn't exist in joined_data_by_group
        if group not in joined_data_by_group:
            joined_data_by_group[group] = df_a.copy()
        else:
            # Outer join based on the 'date' column
            joined_data_by_group[group] = pd.merge(
                joined_data_by_group[group], df_a, how='outer', on=['date']
            )

    # Log the keys in the joined_data_by_group dictionary
    logging.info(f"Keys in joined_data_by_group after processing: {joined_data_by_group.keys()}")

    return joined_data_by_group

# Saves DataFrames to Excel and separate files
def convert_dataframes(joined_data_by_group):
    """
    Converts joined DataFrames

    Parameters:
        joined_data_by_group (dict): Dictionary containing joined DataFrames.

    Returns:
        dict: Dictionary containing modified DataFrames.
    """
    # Create a deep copy of the input dictionary to avoid modifying original data
    joined_data_by_group_copy = deepcopy(joined_data_by_group)

    # Loop through each DataFrame in the dictionary
    for df_name, df_data in joined_data_by_group_copy.items():

        # Loop through each column and convert its type if needed
        for col in df_data.columns:

            # Try converting 'object' dtype columns to datetime
            if df_data[col].dtype == 'object':
                df_data[col] = pd.to_datetime(df_data[col], errors='coerce')

            # Make datetime columns timezone-naive if they are timezone-aware
            if pd.api.types.is_datetime64_any_dtype(df_data[col].dtype):
                if df_data[col].dt.tz is not None:
                    df_data[col] = df_data[col].dt.tz_convert(None)


    return joined_data_by_group_copy

##################################################################################################################################
# Main function to perform all the operations

# Initialize FileManager objects for clean data and documentation
file_manager = FileManager()

# Load the 'apple_data_documentation' Excel sheet into a DataFrame
apple_data = file_manager.load_file(FileDirectory.DOCUMENTATION_PATH, 'apple_data_documentation.xlsx', sheet_name='Summary')

# Define the path where the documentation Excel will be saved
doc_path = os.path.join(FileDirectory.DOCUMENTATION_PATH, "apple_data_documentation2.xlsx")

# Load the datasets
records = file_manager.load_file(FileDirectory.RAW_DATA_PATH, AppleHealth.RECORD_DATA)
activity = file_manager.load_file(FileDirectory.RAW_DATA_PATH, AppleHealth.ACTIVITY_DATA)

# Create a dictionary to store all DataFrames
dataframes_dict = {
    'record': records,
    'activity_summary': activity
}

# Process the 'ActivitySummary' DataFrame separately if needed
process_activity_summary(dataframes_dict)

# Process record data into a new dictionary of DataFrames
record_dict = process_record(dataframes_dict, apple_data)

# Transform the data in the record_dict based on configurations in apple_data
record_dict_transformed = transform_record_dicts(record_dict, apple_data)

# Add the 'ActivitySummary' DataFrame to the transformed records dictionary
record_dict_transformed['activity_summary'] = dataframes_dict['activity_summary'].copy()

# Join DataFrames by group
joined_data_by_group = join_data_by_group(record_dict_transformed, apple_data)

# Standardise date fields in the 'sleep' DataFrame
APPLE_DATE_FIELDS = ['creation_date', 'bed_time', 'awake_time']
standardise_dates(joined_data_by_group['sleep'], APPLE_DATE_FIELDS)

# Convert and save the DataFrames to Excel files
convert_dataframes(joined_data_by_group)



###############################################################

# Save data
# Loop through each key-value pair in joined_data_by_group
for key, df in joined_data_by_group.items():
    # Construct the filename using the key (e.g., if key is 'spending_data', filename will be 'spending_data.xlsx')
    filename = f"apple_{key}.xlsx"
    # Save the DataFrame to an Excel file
    file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, df, filename)

# Define logic documentation (details can be filled in later)
SCRIPT_LOGIC = textwrap.dedent('''\
    Apple Data Documentation
    1
    2
    3
    4
    5
    ''')

# Create an empty dictionary to hold DataFrames for documentation
data_frames = {}
# Add raw DataFrames from dataframes_dict to the documentation dictionary
data_frames['raw'] = {key: df for key, df in dataframes_dict.items()}
# Add cleaned DataFrames (if you have any, you can populate this part)
data_frames['clean'] = {}  # Empty for now, you can fill this in
# Add transformed DataFrames from joined_data_by_group to the documentation dictionary
data_frames['transformed'] = {key: df for key, df in joined_data_by_group.items()}

# Path for the documentation file
doc_path = os.path.join(FileDirectory.DOCUMENTATION_PATH, "apple_documentation2.xlsx")

# Initialize documentation object
documenter = DataFrameDocumenter(doc_path, SCRIPT_LOGIC)

# Loop through each category and DataFrame for documentation
for category, frames in data_frames.items():
    for name, df in frames.items():
        # Document each DataFrame in Excel, with the category and DataFrame name
        documenter.document_data_excel(df, category, name)

# Save the Excel documentation
documenter.save()

