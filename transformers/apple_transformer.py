"""
This script orchestrates the transformation and documentation of Apple Health data.

Key Processes:
1. Loading Data:
   - Utilises FileManager to load 'records' and 'activity' data from the specified raw data directory.

2. Data Processing:
   - Processes 'records' data using the `process_record` function, which returns a dictionary of DataFrames.
   - Applies transformation to the record dictionaries based on configurations in `apple_data`.

3. Activity Data Processing:
   - Handles 'ActivitySummary' DataFrame separately through the `process_activity_summary` function.
   - Adds the processed 'ActivitySummary' DataFrame to the transformed records dictionary.

4. Data Joining:
   - Joins different DataFrames by group using the `join_data_by_group` function.
   - Standardises date fields in the 'sleep' DataFrame using the `standardise_dates` and `DataStandardiser` functions.

5. Data Saving:
   - Iterates through the grouped data, saving each DataFrame to an Excel file in the clean data directory.

6. Data Documentation:
   - Constructs a documentation dictionary with raw, clean, and transformed DataFrames.
   - Utilises DataFrameDocumenter to document each DataFrame category in an Excel file.
   - Saves the documented Excel file to a specified documentation path.

7. Output:
   - Returns two dictionaries: one with transformed data and another with joined data by group.

Usage:
- This script is executed as the main program to process and document Apple Health data.
- It ensures that data is not only transformed but also thoroughly documented for future reference.
"""

# Import the required libraries
import os  # For operating system related functionality
from typing import Dict

import pandas as pd  # For data manipulation and analysis
import pytz
from pandas import DataFrame

from config import Settings

# Custom imports
from constants import AppleHealth, FileDirectory
from utility.documentation import DataFrameDocumenter  # For documenting data frames
from utility.file_manager import FileManager
from utility.logging import setup_logging
from utility.standardise_data import DataStandardiser  # For standardising data fields
from utility.standardise_dates import (
    standardise_dates,  # For standardising date formats
)
from utility.utils import generate_filename

# Initialize logging
logger = setup_logging()


##################################################################################################################################
# Function to Process 'ActivitySummary' in the DataFrame Dictionary
def process_activity_summary(df):
    """
    Processes the 'ActivitySummary' DataFrame
    Keeps only specific columns and updates the dictionary.
    """

    # Log that we are starting to transform 'ActivitySummary'
    logger.info("Transforming ActivitySummary")

    # Create a copy of the 'ActivitySummary' DataFrame
    df_copy = df.copy()

    # Define the columns we want to keep
    columns_to_keep = [
        AppleHealth.DATE_COMPONENTS,
        AppleHealth.ACTIVE_ENERGY_BURNED,
        AppleHealth.APPLE_EXERCISE_TIME,
        AppleHealth.APPLE_STAND_HOURS,
    ]

    # Keep only the specified columns
    df_copy = df_copy[columns_to_keep]

    # Convert specific columns to numeric data types, handling errors by coercing them to NaN
    for col in columns_to_keep[1:]:
        df_copy[col] = pd.to_numeric(df_copy[col], errors="coerce")

    # Rename 'dateComponents' to 'date'
    df_copy.rename(columns={columns_to_keep[0]: AppleHealth.DATE}, inplace=True)

    return df_copy


# Process records and transform them
def process_record(df):
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
        Transforms the DataFrame with various operations like timesone conversion, data extraction, and so on.

        Parameters:
            df (pd.DataFrame): The DataFrame to transform.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        # Make a copy of the DataFrame to prevent modifying the original
        df = df.copy()

        # Columns that need to be converted to datetime
        date_cols = [
            AppleHealth.CREATION_DATE,
            AppleHealth.START_DATE,
            AppleHealth.END_DATE,
        ]

        # Convert these columns to datetime and localise the timesone to London time
        london_tz = pytz.timezone(Settings.TIMEZONE)
        df[date_cols] = (
            df[date_cols]
            .apply(pd.to_datetime)
            .apply(lambda x: x.dt.tz_convert(london_tz))
        )

        # Extract only the date part from 'startDate'
        df[AppleHealth.DATE] = df[AppleHealth.CREATION_DATE].dt.strftime(
            Settings.DATE_FORMAT
        )

        # Extract the hour from 'startDate'
        df[AppleHealth.HOUR] = df[AppleHealth.START_DATE].dt.hour

        # Store the original 'value' into a new column
        df[AppleHealth.ORIGINAL_VALUE] = df[AppleHealth.VALUE]

        # Convert 'value' to numeric, handling errors by coercing them to NaN and then filling NaNs with 1.0
        df[AppleHealth.VALUE] = pd.to_numeric(
            df[AppleHealth.VALUE], errors="coerce"
        ).fillna(1.0)

        # Remove leading and trailing spaces from 'sourceName'
        df[AppleHealth.SOURCE_NAME] = df[AppleHealth.SOURCE_NAME].str.strip()

        return df

    # Transform the DataFrame for the 'Record' key
    transformed_df = transform_df(df)

    # Create a set for quick look-up
    unique_types = set(transformed_df[AppleHealth.TYPE_FIELD].unique())

    # Initialise the dictionary to hold the preview tables
    preview_tables = {}

    # Loop over each element of interest
    for elem in AppleHealth.RECORD_ELEMENTS:
        if elem in unique_types:
            # Create a variable name by replacing invalid characters
            var_name = elem.replace("-", "_").replace(" ", "_")

            # Filter rows by 'type'
            filtered_df = transformed_df[transformed_df[AppleHealth.TYPE_FIELD] == elem]

            preview_tables[var_name] = filtered_df
            # Drop duplicates based on 'startDate' and 'endDate'
            # preview_tables[var_name] = filtered_df.drop_duplicates(subset=['start_date', 'end_date'])
        else:
            logger.info(f"The specified type '{elem}' does not exist in the DataFrame.")

    return preview_tables


def subset_by_priority(df):
    df[AppleHealth.PRIORITY] = df[AppleHealth.SOURCE_NAME].map(
        AppleHealth.PRIORITY_DICT
    )

    # Sort and group by 'creation_date' to find the minimum priority for each date
    df.sort_values(by=[AppleHealth.CREATION_DATE, AppleHealth.PRIORITY], inplace=True)
    df = df[
        df[AppleHealth.PRIORITY]
        == df.groupby(AppleHealth.DATE)[AppleHealth.PRIORITY].transform("min")
    ]

    return df


##################################################################################################################################
# Transform records
def handle_sleep_analysis(df):
    """
    Transforms the SleepAnalysis DataFrame.
    """

    # Initialise a DataFrame with sleep analysis data
    df = df.copy()

    # =============================================================================
    #     df = record_dict['SleepAnalysis']
    #     # For each unique data point, keep data for only one source
    #     df = subset_by_priority(df)
    #
    # =============================================================================
    # Extract the relevant part from 'original_value' column, remove everything before 'SleepAnalysis'
    df[AppleHealth.ORIGINAL_VALUE] = (
        df[AppleHealth.ORIGINAL_VALUE].str.split("SleepAnalysis").str.get(-1)
    )

    # Convert 'start_date', 'end_date', and 'creation_date' to datetime
    # df[['start_date', 'end_date', 'creation_date']] = df[['start_date', 'end_date', 'creation_date']].apply(pd.to_datetime)

    # Calculate 'duration' in hours and extract just the date part from 'creation_date'
    df[AppleHealth.DURATION] = (
        df[AppleHealth.END_DATE] - df[AppleHealth.START_DATE]
    ).dt.total_seconds() / 3600

    # Group by 'date' and 'original_value', then sum the 'duration' for each group
    pivot_df = df.pivot_table(
        index=AppleHealth.DATE,
        columns=AppleHealth.ORIGINAL_VALUE,
        values=AppleHealth.DURATION,
        aggfunc="sum",
    ).reset_index()

    # Add 'bed_time', 'awake_time', and 'source_name' columns to pivot_df
    grouped = df.groupby(AppleHealth.DATE)
    pivot_df[AppleHealth.BED_TIME] = (
        grouped[AppleHealth.START_DATE].min().reset_index()[AppleHealth.START_DATE]
    )
    pivot_df[AppleHealth.AWAKE_TIME] = (
        grouped[AppleHealth.END_DATE].max().reset_index()[AppleHealth.END_DATE]
    )
    pivot_df[AppleHealth.SOURCE_NAME] = (
        grouped[AppleHealth.SOURCE_NAME].first().reset_index()[AppleHealth.SOURCE_NAME]
    )
    pivot_df[AppleHealth.TIME_IN_BED] = (
        pivot_df[AppleHealth.AWAKE_TIME] - pivot_df[AppleHealth.BED_TIME]
    ).dt.total_seconds() / 3600

    # Step 1: Apply value from ‘AsleepUnspecified’ to ‘InBed’ if ‘InBed’ is NaN
    pivot_df["InBed"] = pivot_df["InBed"].fillna(pivot_df["AsleepUnspecified"])

    # Step 2: Adjust ‘Awake’ based on ‘InBed’ and ‘time_in_bed’
    condition = pivot_df["Awake"].isna() & (
        pivot_df["InBed"] != pivot_df[AppleHealth.TIME_IN_BED]
    )
    pivot_df.loc[condition, "Awake"] = (
        pivot_df[AppleHealth.TIME_IN_BED] - pivot_df["InBed"]
    )

    # Step 3: Rename ‘InBed’ to ‘total_sleep’
    pivot_df.rename(columns={"InBed": "total_sleep"}, inplace=True)

    # Filter the DataFrame to keep only the rows where 'Awake' is non-negative or NaN
    pivot_df = pivot_df[(pivot_df["Awake"] >= 0) | pd.isna(pivot_df["Awake"])]

    return pivot_df


def transform_record_dicts(record_dict: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """
    Transforms record dictionaries based on instructions in apple_data DataFrame.
    """

    # Initialise an empty dictionary to hold new DataFrames
    new_dataframe_dict = {}

    for df_name in AppleHealth.RECORD_ELEMENTS:
        # Get the original DataFrame
        original_df = record_dict.get(df_name)

        # Skip if DataFrame does not exist
        if original_df is None:
            continue

        # Create a copy to prevent modifying the original DataFrame
        new_df = original_df.copy()

        # Extract source-related information
        source_info = AppleHealth.RECORD_TRANSFORMATION_LOGIC.get(df_name, {})
        agg_type = source_info.get("agg_type")[0].capitalize()
        fields = source_info.get("fields")
        timeframes = source_info.get("timeframe")

        # Initialise an empty dictionary for aggregation
        agg_dict = {}

        logger.info(f"Transforming {df_name}")

        # For each unique data point, keep data for only one source
        new_df = subset_by_priority(new_df)

        # Apply transformations specific to each DataFrame type
        if df_name == "MindfulSession":
            new_df.loc[:, AppleHealth.DURATION] = (
                new_df[AppleHealth.END_DATE] - new_df[AppleHealth.START_DATE]
            ).dt.total_seconds() / 60
            agg_dict.update(
                dict(
                    zip(
                        fields,
                        [(AppleHealth.DURATION, "sum"), (AppleHealth.VALUE, "count")],
                    )
                )
            )
            new_df = new_df.groupby(AppleHealth.DATE).agg(**agg_dict).reset_index()

        elif df_name == "SleepAnalysis":
            new_df = handle_sleep_analysis(new_df)

        else:
            agg_dict = {
                field: (AppleHealth.VALUE, agg_type.lower()) for field in fields
            }
            new_df = new_df.groupby(timeframes).agg(**agg_dict).reset_index()

        # Store the transformed DataFrame
        new_dataframe_dict[df_name] = new_df

    return new_dataframe_dict


##################################################################################################################################
# Joins data by group
def join_data_by_group(complete_dict):
    """
    Join multiple DataFrames by group based on a configuration DataFrame.

    Parameters:
        complete_dict (dict): Dictionary containing DataFrames to join.
        apple_data (pd.DataFrame): DataFrame containing configuration info.

    Returns:
        dict: Dictionary containing joined DataFrames.
    """

    merger_logic = {
        **AppleHealth.RECORD_TRANSFORMATION_LOGIC,
        **AppleHealth.ACTIVITY_TRANSFORMATION_LOGIC,
    }

    # Create a copy of the complete_dict to avoid modifying the original
    complete_dict_copy = {key: df.copy() for key, df in complete_dict.items()}

    # Initialise empty dictionary to store joined data
    joined_data_by_group = {}

    for key in complete_dict_copy.keys():
        # Deep copy of DataFrame in the complete_dict
        df_a = complete_dict_copy[key]

        # Round all numerical columns to 2 decimal places, leave 0 and NaN unchanged
        for col in df_a.select_dtypes(include=["float64"]).columns:
            df_a[col] = df_a[col].apply(
                lambda x: round(x, 2) if not pd.isna(x) and x != 0 else x
            )

        # If DataFrame is empty or key is not in configuration, skip to next iteration
        if df_a.empty or key not in merger_logic:
            logger.info(
                f"Skipping key {key} because it is empty or not in the configuration."
            )
            continue

        # Fetch group name from all_df_dict
        group = merger_logic[key]["group"][0]

        # Initialise group key-value if it doesn't exist in joined_data_by_group
        if group not in joined_data_by_group:
            joined_data_by_group[group] = df_a.copy()
        else:
            # Outer join based on the 'date' column
            joined_data_by_group[group] = pd.merge(
                joined_data_by_group[group], df_a, how="outer", on=[AppleHealth.DATE]
            )

    # Log the keys in the joined_data_by_group dictionary
    logger.info(
        f"Keys in joined_data_by_group after processing: {joined_data_by_group.keys()}"
    )

    return joined_data_by_group


##################################################################################################################################
# Main function to perform all the operations
def apple_transformer():
    """
    Orchestrates the extraction, transformation, and storage of Apple data.

    Returns:
        tuple: Two dictionaries containing transformed and grouped data.
    """

    # Initialise FileManager objects for clean data and documentation
    file_manager = FileManager()

    # Load the datasets
    records = file_manager.load_file(
        FileDirectory.RAW_DATA_PATH, AppleHealth.RECORD_DATA
    )
    activity = file_manager.load_file(
        FileDirectory.RAW_DATA_PATH, AppleHealth.ACTIVITY_DATA
    )

    # Process record data into a new dictionary of DataFrames
    record_dict = process_record(records)
    # Transform the data in the record_dict based on configurations in apple_data
    record_dict_transformed = transform_record_dicts(record_dict)

    # Process the 'ActivitySummary' DataFrame separately if needed
    activity_df = process_activity_summary(activity)

    # Add the 'ActivitySummary' DataFrame to the transformed records dictionary
    record_dict_transformed[AppleHealth.ACTIVITY_ELEMENT] = activity_df.copy()

    # Join DataFrames by group
    joined_data_by_group = join_data_by_group(record_dict_transformed)

    standardise_dates(
        joined_data_by_group["sleep"], [AppleHealth.BED_TIME, AppleHealth.AWAKE_TIME]
    )
    standardiser = DataStandardiser()
    standardiser.standardise_df(joined_data_by_group["sleep"])

    ###############################################################

    # Save data
    # Loop through each key-value pair in joined_data_by_group
    for key, df in joined_data_by_group.items():
        # Save the DataFrame to an Excel file
        file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, df, f"apple_{key}.xlsx")

    # Create an empty dictionary to hold DataFrames for documentation
    data_frames = {}
    # Add raw DataFrames from dataframes_dict to the documentation dictionary
    # Assuming 'records' and 'activity' are your DataFrames
    data_frames["raw"] = {"records": records, "activity": activity}
    # Add cleaned DataFrames (if you have any, you can populate this part)
    data_frames["clean"] = {}  # Empty for now, you can fill this in
    # Add transformed DataFrames from joined_data_by_group to the documentation dictionary
    data_frames["transformed"] = {key: df for key, df in joined_data_by_group.items()}

    # Define the path where the documentation Excel will be saved
    doc_path = os.path.join(
        FileDirectory.DOCUMENTATION_PATH,
        generate_filename(AppleHealth.DOCUMEMTATION_DATA),
    )

    # Initialise documentation object
    documenter = DataFrameDocumenter(doc_path, AppleHealth.SCRIPT_LOGIC)

    # Loop through each category and DataFrame for documentation
    for category, frames in data_frames.items():
        for name, df in frames.items():
            # Document each DataFrame in Excel, with the category and DataFrame name
            documenter.document_data_excel(df, category, name)

    # Save the Excel documentation
    documenter.save()

    ###############################################################

    # Return the transformed and grouped data dictionaries
    return record_dict_transformed, joined_data_by_group


# The following line allows you to run this code only when this script is executed, not when it's imported
if __name__ == "__main__":
    # Run the main apple_transformer function and store its outputs in grouped_dict and transform_dict
    grouped_dict, transform_dict = apple_transformer()
