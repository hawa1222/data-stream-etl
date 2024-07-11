"""
This script is designed to perform post-load data validation checks on data imported into a MySQL database.
It compares pre-loaded data (original data) with the data retrieved from the database after the loading process.

Imports:
    - sys: Used for manipulating Python runtime environment.
    - random: Used for generating random selections.
    - logging: Used for logging information and errors.
    - pandas: Used for handling data in DataFrame format.
    - decimal.Decimal: Used for precise decimal arithmetic.

Configuration:
    - Prevents Python from writing bytecode files (.pyc).
    - Adds a specific path to the system path for custom imports.

Custom Imports:
    - Imports various constants, configuration values, and utility classes like FileManager, setup_logging,
      and DatabaseHandler.

Functionality:
    1. Initializes logging for the script.
    2. Defines a function `compare_random_rows` to compare rows from two DataFrames based on primary keys.
       This function logs data type mismatches and value discrepancies.
    3. Defines a function `post_load_checks` to perform dimensional and data integrity checks on multiple datasets.
    4. The `post_load` function:
       - Defines a mapping of dataset names to file names.
       - Initializes FileManager to load pre-load data.
       - Initializes DatabaseHandler to fetch data from the database.
       - Converts date fields to appropriate formats for comparison.
       - Executes post-load checks using the defined functions.
    5. The script executes the `post_load` function if run as the main program.

Usage:
    Run this script to validate data integrity after loading data into a MySQL database. Ensure that all necessary
    modules and custom classes are accessible. Update the datasets mapping and database configurations as needed.

Note:
    The script relies on the correct configuration of the database and the presence of the specified files and data formats.
    It's essential to update the configurations and file paths according to your environment and data structure.
"""

import random
from decimal import Decimal

import pandas as pd

from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from constants import Daylio, FileDirectory, Google, Spend, Strava
from utility.database_manager import DatabaseHandler
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def compare_random_rows(df_db, df_pre_load, primary_keys, table_name):
    """
    Compares rows between pre-loaded and database DataFrames based on primary key(s).
    Logs data type information alongside value mismatch errors for better diagnosis.
    """
    num_rows = len(df_pre_load)
    sample_size = num_rows if num_rows < 10 else 10  # Determine sample size

    # Handling composite or single primary keys
    if isinstance(primary_keys, list):
        unique_key_combinations = df_pre_load[primary_keys].drop_duplicates()
        sampled_or_all_keys = (
            unique_key_combinations.sample(n=sample_size)
            if num_rows > 10
            else unique_key_combinations
        )
    else:
        unique_keys = df_pre_load[primary_keys].unique()
        sampled_or_all_keys = (
            random.sample(list(unique_keys), sample_size)
            if num_rows > 10
            else unique_keys
        )

    for _, key_row in sampled_or_all_keys.iterrows():
        # Construct query condition based on primary key(s)
        query_condition = (
            " & ".join([f'`{pk}` == "{key_row[pk]}"' for pk in primary_keys])
            if isinstance(primary_keys, list)
            else f'`{primary_keys}` == "{key_row[primary_keys]}"'
        )

        # Query both pre-load and post-load df
        db_row = df_db.query(query_condition)
        pre_load_row = df_pre_load.query(query_condition)

        # Compare values in each field
        for field in df_pre_load.columns:
            pre_load_value = (
                pre_load_row[field].iloc[0] if not pre_load_row.empty else None
            )
            db_value = db_row[field].iloc[0] if not db_row.empty else None

            # Handle Decimal vs. Float comparison
            if isinstance(pre_load_value, float) and isinstance(db_value, Decimal):
                pre_load_value = Decimal(str(pre_load_value))

            if pd.isna(pre_load_value) and pd.isna(db_value):
                continue  # Skip if both values are NaN

            if pre_load_value != db_value or type(pre_load_value) != type(db_value):
                # Log error with table name, mismatch details, and data types
                logger.error(
                    f"Mismatch found in table '{table_name}' for {primary_keys} {key_row[primary_keys]}: Field '{field}' has pre-load value '{pre_load_value}' (type: {type(pre_load_value).__name__}) and post-load value '{db_value}' (type: {type(db_value).__name__})"
                )
                return False

    # Log success with table name
    logger.info(
        f"All compared rows for {primary_keys} in table '{table_name}' match between pre-load and post-load data."
    )
    return True


def post_load_checks(pre_Load_dfs, db_dataframes):
    """
    Performs post-load checks for each dataset, including table names in logs.
    """
    for table_name, df_pre_load in pre_Load_dfs.items():
        df_db = db_dataframes.get(table_name, pd.DataFrame())

        # Log dimension mismatch with table name
        if df_pre_load.shape[0] != df_db.shape[0]:
            logger.error(
                f"Dimension mismatch in table '{table_name}': pre-load count {df_pre_load.shape[0]}, post-load count {df_db.shape[0]}"
            )
            continue  # Proceed to next dataset

        # Determine primary key(s) for each table
        primary_key = (
            ["date", "hour"]
            if "date" in df_db.columns and "hour" in df_db.columns
            else [df_db.columns[0]]
        )

        # Compare random rows with table name included in the function call
        if not compare_random_rows(df_db, df_pre_load, primary_key, table_name):
            continue  # Proceed to next dataset

        # Log success with table name
        logger.info(f"Post-load checks passed for table '{table_name}'")


def post_load():
    """
    Main function to perform post-load data validation checks.
    """
    logger.info("Starting post-load data validation checks...")

    try:
        YT = f"{Google.DATA_KEY}_enriched"
        datasets = {
            "apple_walking_metrics",
            "apple_daily_activity",
            "apple_blood_glucose",
            "apple_heart_rate",
            "apple_fitness_metrics",
            "apple_low_hr_events",
            "apple_running_metrics",
            "apple_sleep",
            "apple_steps",
            Daylio.ACTIVITY_DATA,
            Daylio.MOOD_DATA,
            Spend.DATA_KEY,
            Strava.PERFORMANCE_DATA,
            Strava.ACTIVITY_DATA,
            YT,
        }

        file_manager = FileManager()

        pre_Load_dfs = {
            name: file_manager.load_file(FileDirectory.CLEAN_DATA_PATH, name)
            for name in datasets
        }

        db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

        db_dataframes = db_handler.fetch_data(datasets)

        # Convert 'date' field to string for all keys containing 'apple'
        for key in db_dataframes:
            if "apple" in key:
                db_dataframes[key]["date"] = db_dataframes[key]["date"].astype(str)

        date_fields = ["date_time", "published_at", "bed_time", "awake_time"]

        for key, df in db_dataframes.items():
            for field in date_fields:
                if field in df.columns:
                    # Convert to datetime, then to ISO 8601 format, and finally to string
                    db_dataframes[key][field] = (
                        pd.to_datetime(df[field])
                        .dt.tz_localize(None)
                        .dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                    )

        for key, df in pre_Load_dfs.items():
            for field in date_fields:
                if field in df.columns:
                    # Convert to datetime with standardized time zone
                    converted = pd.to_datetime(df[field], errors="coerce", utc=True)

                    # Check if conversion was successful (not all values are NaT)
                    if converted.notna().any():
                        # Format to string without time zone information
                        pre_Load_dfs[key][field] = converted.dt.tz_convert(
                            None
                        ).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                    else:
                        print(
                            f"Conversion to datetime failed or not applicable for field '{field}' in DataFrame '{key}'"
                        )

        post_load_checks(pre_Load_dfs, db_dataframes)

    except Exception as e:
        logger.error(f"Error occurred in post_load: {str(e)}")


if __name__ == "__main__":
    post_load()
