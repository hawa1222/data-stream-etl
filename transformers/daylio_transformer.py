from datetime import datetime

import pandas as pd
from zoneinfo import ZoneInfo

from constants import Daylio, FileDirectory
from utility.clean_dates import parse_date
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def add_uk_timezone(date_string):
    dt = datetime.strptime(date_string, "%Y-%m-%d %I:%M%p")  # 2024-03-29 3:04PM

    uk_time = dt.replace(tzinfo=ZoneInfo("Europe/London"))  # Add UK timezone

    return uk_time.strftime("%Y-%m-%d %I:%M%p %Z")  # 2024-03-29 3:04PM BST


def clean_data(df):
    """
    Cleans data by concatenating & cleaning date and time fields,
    and parsing dates, and selecting only required fields.

    Parameters:
        df (DataFrame): Raw DataFrame to clean.

    Returns:
        DataFrame: Cleaned DataFrame.
    """

    logger.info("Cleaning data...")

    try:
        df[Daylio.DATE_TIME] = (
            df[Daylio.DATE]
            + " "
            + df[Daylio.TIME]
            .str.upper()
            .str.replace(r"[^a-zA-Z0-9:\- ]", "", regex=True)
            .str.replace("‚ÄØ", "", regex=False)
        )  # Concatenate date and time fields

        logger.info(f"Stardardising {Daylio.DATE_TIME} field...")
        logger.info(f"Sample before cleaning: {df[Daylio.DATE_TIME].head(2).to_list()}")
        df[Daylio.DATE_TIME] = df[Daylio.DATE_TIME].apply(add_uk_timezone)
        logger.info(f"Sample before parsing: {df[Daylio.DATE_TIME].head(2).to_list()}")
        df[Daylio.DATE_TIME] = df[Daylio.DATE_TIME].apply(parse_date)  # Parse dates
        logger.info(f"Sample after cleaning: {df[Daylio.DATE_TIME].head(2).to_list()}")

        df = df[Daylio.CLEAN_FIELDS]

        logger.info("Successfully cleaned data")

        return df

    except Exception as e:
        logger.error(f"Error occurred while cleaning DataFrame: {str(e)}")
        raise


def transform_data(df):
    """
    Partially normalise cleaned data into two DataFrames: mood & activities.

    Parameters:
        df : Cleaned DataFrame.

    Returns:
        Tuple[DataFrame]: Tuple containing mood & activities DataFrames.
    """

    logger.info("Transforming data...")

    df_mood = (
        df.drop(columns=[Daylio.ACTIVITY])  # Drop 'activities' field
        .drop_duplicates(subset=[Daylio.DATE_TIME])
        .reset_index(drop=True)
    )

    df_activities = df[[Daylio.DATE_TIME, Daylio.ACTIVITY]]

    df_activities.loc[:, Daylio.ACTIVITY] = df_activities[Daylio.ACTIVITY].apply(
        lambda x: str(x).split(" | ") if pd.notna(x) else []
    )  # Split  'activities' field by ' | '

    df_activities = (
        df_activities.explode(Daylio.ACTIVITY)  # Explode 'activities' field
        .dropna(subset=[Daylio.ACTIVITY])
        .reset_index(drop=True)
    )
    df_activities[Daylio.ID] = df_activities.index + 1  # Add 'id' field

    logger.info("Partially normalised data into two tables: df_mood, df_activities")

    return df_mood, df_activities


def daylio_transformer():
    """
    Main function to load Daylio Data, clean & transform, split into two DataFrames and save to local storage.
    """
    logger.info("!!!!!!!!!!!! daylio_transformer.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()

        df = file_manager.load_file(FileDirectory.RAW_DATA_PATH, Daylio.DATA_KEY)

        clean_df = clean_data(df)

        df_mood, df_activities = transform_data(clean_df)
        datasets = {
            Daylio.DATA_KEY: clean_df,
            Daylio.MOOD_DATA: df_mood,
            Daylio.ACTIVITY_DATA: df_activities,
        }

        # Use a loop to save each dataframe
        for file_name, dataframe in datasets.items():
            file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, file_name, dataframe)

    except Exception as e:
        logger.error(f"Error occurred in daylio_transformer: {str(e)}")


if __name__ == "__main__":
    daylio_transformer()
