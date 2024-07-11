from constants import FileDirectory, Google
from utility.clean_dates import parse_date
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def manipulate_activity_data(df):
    """
    Transforms YouTube activity data:

    1. Standardise date field.
    2. Extract content_id, content_thumbnail, and channel_id from content_url and channel_url.
    3. Add source field.
    4. Subset DataFrame to only include required fields.

    Parameters:
        df: Raw DataFrame containing YouTube activity data.

    Returns:
        DataFrame: Manipulated DataFrame.
    """

    try:
        logger.info("Cleaning activity data...")

        logger.info(f"Stardardising {Google.DATE} field...")
        logger.info(f"Sample before cleaning: {df[Google.DATE].head(2).to_list()}")
        df[Google.DATE] = df[Google.DATE].apply(parse_date)
        logger.info(f"Sample after cleaning: {df[Google.DATE].head(2).to_list()}")

        df[Google.CONTENT_ID] = (
            df[Google.CONTENT_URL]
            .astype(str)
            .str.extract(r"(?:v=|list=|\/post\/)([^&]*)")[0]
            .str.split("&")
            .str[0]
        )  # Extract content_id from content_url

        df[Google.CONTENT_THUMBNAIL] = (
            "https://img.youtube.com/vi/" + df[Google.CONTENT_ID] + "/maxresdefault.jpg"
        )  # Extract content_thumbnail from content_id

        df[Google.CHANNEL_ID] = (
            df[Google.CHANNEL_URL].str.split("channel/").str[1].str.split("&").str[0]
        )  # Extract channel_id from channel_url

        logger.info(
            f"Succesfully created '{Google.CONTENT_ID}', '{Google.CONTENT_THUMBNAIL}', and '{Google.CHANNEL_ID}' fields"
        )

        df[Google.CONTENT_DESC] = None
        df[Google.CHANNEL_DESC] = None
        df[Google.CHANNEL_THUMBNAIL] = None
        df[Google.SOURCE] = Google.SOURCE_VALUE[1]

        df = df[Google.ACTIVITY_FIELDS]

        logger.info("Successfully cleaned HTML data")

        return df

    except Exception as e:
        logger.error(f"Error occurred in manipulate_activity_data: {str(e)}")
        raise


def youtube_html_transformer():
    """
    Main function to load YouTube HTML Data, clean & transform, and save to local storage.
    """
    logger.info("!!!!!!!!!!!! youtube_html_transformer.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()

        parsed_html = file_manager.load_file(
            FileDirectory.RAW_DATA_PATH, Google.DATA_KEY
        )

        act_df = manipulate_activity_data(parsed_html)

        file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, Google.DATA_KEY, act_df)

    except Exception as e:
        logger.error(f"Error occurred in youtube_html_transformer: {str(e)}")


if __name__ == "__main__":
    youtube_html_transformer()
