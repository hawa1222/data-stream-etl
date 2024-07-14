"""
Script designed to perform post-load data validation checks on data loaded into MySQL database.

    Compares pre-loaded data (original data) with data retrieved from database after loading process.
    Validation checks include dimensional and data integrity checks to ensure data consistency and accuracy.

Functions:
    `post_load` to perform post-load data validation checks.
    `compare_random_rows` to compare rows from two DataFrames based on primary keys.
    `post_load_checks` to perform dimensional and data integrity checks on multiple datasets.

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

        # Compare random rows with table name included in function call
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

        def convert_date_fields(df, is_apple_data):
            date_fields = [
                "date",
                "date_time",
                "published_at",
                "bed_time",
                "awake_time",
            ]

            for field in date_fields:
                if field in df.columns:
                    if field == "date" and is_apple_data:
                        df[field] = df[field].astype(str)
                    else:
                        df[field] = pd.to_datetime(
                            df[field], errors="coerce", utc=True
                        ).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            return df

        for key, df in db_dataframes.items():
            is_apple_data = "apple" in key
            db_dataframes[key] = convert_date_fields(df, is_apple_data)

        post_load_checks(pre_Load_dfs, db_dataframes)

    except Exception as e:
        logger.error(f"Error occurred in post_load: {str(e)}")


if __name__ == "__main__":
    post_load()
