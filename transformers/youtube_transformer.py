import numpy as np
import pandas as pd

from constants import FileDirectory, Google
from utility.clean_dates import parse_date
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def get_best_url(row):
    """
    Finds the best available URL for a given row based on hierarchy.

    Parameters:
    - row: Row from a DataFrame containing various types of URLs.

    Returns:
    - str/np.nan: Best available URL or np.nan if none found.
    """
    # Loop through URL types in specified order to find best available
    for url_type in [
        "snippet.thumbnails.maxres.url",
        "snippet.thumbnails.standard.url",
        "snippet.thumbnails.high.url",
        "snippet.thumbnails.medium.url",
        "snippet.thumbnails.default.url",
    ]:
        if pd.notna(row.get(url_type)):
            return row[url_type]
    return np.nan  # Return np.nan if no URL is found


def likes_transformer(df):
    """
    Transforms YouTube likes data:

    1. Rename fields.
    2. Add source and activity_type fields.
    3. Add content_url and channel_url fields.
    4. Standardise date field.
    5. Add content_thumbnail field.
    6. Subset DataFrame to only include required fields.

    Parameters:
        df: DataFrame containing the likes data.

    Returns:
        DataFrame: Transformed DataFrame.
    """
    try:
        logger.debug("Transforming likes data...")

        df = df.rename(columns=Google.LIKES_MAPPING)

        df[Google.SOURCE] = Google.SOURCE_VALUE[0]
        df[Google.ACTIVITY_TYPE] = Google.ACTIVITY_TYPES[0]

        df[Google.CONTENT_URL] = "https://www.youtube.com/watch?v=" + df[Google.CONTENT_ID]
        df[Google.CHANNEL_URL] = "https://www.youtube.com/channel/" + df[Google.CHANNEL_ID]

        logger.debug(f"Stardardising {Google.DATE} field...")
        logger.debug(f"Sample before cleaning: {df[Google.DATE].head(2).to_list()}")
        df[Google.DATE] = df[Google.DATE].apply(parse_date)
        logger.debug(f"Sample after cleaning: {df[Google.DATE].head(2).to_list()}")

        df[Google.CONTENT_THUMBNAIL] = df.apply(get_best_url, axis=1)

        df = df[list(Google.LIKES_MAPPING.values())]

        logger.debug(f"Renamed / added fields: [{', '.join(Google.LIKES_MAPPING.values())}]")

        logger.info("Successfully transformed likes data")

        return df

    except Exception as e:
        logger.error(f"Error occurred in likes_transformer: {str(e)}")
        raise


def subs_transformer(df):
    """
    Transform YouTube subs data:

    1. Rename fields.
    2. Add source and activity_type fields.
    3. Add channel_url field.
    4. Standardise date field.
    5. Add channel_thumbnail field.
    6. Subset DataFrame to only include required fields.

    Parameters:
        df: DataFrame containing the subs data.

    Returns:
        DataFrame: Transformed DataFrame.
    """
    try:
        logger.debug("Transforming subs data...")

        df = df.rename(columns=Google.SUBS_MAPPING)

        df[Google.SOURCE] = Google.SOURCE_VALUE[0]
        df[Google.ACTIVITY_TYPE] = Google.ACTIVITY_TYPES[2]

        df[Google.CHANNEL_URL] = "https://www.youtube.com/channel/" + df[Google.CHANNEL_ID]

        logger.debug(f"Stardardising {Google.DATE} field...")
        logger.debug(f"Sample before cleaning: {df[Google.DATE].head(2).to_list()}")
        df[Google.DATE] = df[Google.DATE].apply(parse_date)
        logger.debug(f"Sample after cleaning: {df[Google.DATE].head(2).to_list()}")

        df[Google.CHANNEL_THUMBNAIL] = df.apply(get_best_url, axis=1)

        df = df[list(Google.SUBS_MAPPING.values())]

        logger.debug(f"Renamed / added fields: [{', '.join(Google.SUBS_MAPPING.values())}]")

        logger.info("Successfully transformed subs data")

        return df

    except Exception as e:
        logger.error(f"Error occurred in subs_transformer: {str(e)}")
        raise


def youtube_transformer():
    """
    Main function to load YouTube Data, clean & transform it, and save to local storage.
    """
    logger.info("!!!!!!!!!!!! youtube_transformer.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()

        lk_df = file_manager.load_file(FileDirectory.RAW_DATA_PATH, Google.LIKES_DATA)
        lk_df = likes_transformer(lk_df)

        subs_df = file_manager.load_file(FileDirectory.RAW_DATA_PATH, Google.SUBS_DATA)
        subs_df = subs_transformer(subs_df)

        file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, Google.LIKES_DATA, lk_df)
        file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, Google.SUBS_DATA, subs_df)

    except Exception as e:
        logger.error(f"Error occurred in youtube_transformer: {str(e)}")


if __name__ == "__main__":
    youtube_transformer()
