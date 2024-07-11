import pandas as pd

from constants import FileDirectory, Spend
from utility import redis_manager, s3_manager
from utility.clean_data import CleanData
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def spend_extractor():
    """
    Main function to load Spend Data, drop NAs, standardise field names, update cache,
    save to S3 and local storage.
    """
    logger.info("!!!!!!!!!!!! spend_extractor.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()

        cached_data = redis_manager.get_cached_data(Spend.DATA_KEY)  # Fetch cached data

        if cached_data is not None:  # Check cached data exists
            spend_data = pd.DataFrame(cached_data)  # Convert to df
            logger.info(f"Successfully fetched {len(spend_data)} total entries")
        else:
            spend_data = file_manager.load_file(
                FileDirectory.MANUAL_EXPORT_PATH,
                Spend.DATA_KEY,
                extension="xlsm",
                sheet_name=Spend.RAW_SHEET_NAME,
            )  # Load Spend data

            spend_data = CleanData.clean_data(spend_data, 3)

            # Cache new data, upload to S3
            redis_manager.update_cached_data(Spend.DATA_KEY, spend_data)
            s3_manager.post_data_to_s3(Spend.DATA_KEY, spend_data, overwrite=True)

        file_manager.save_file(FileDirectory.RAW_DATA_PATH, Spend.DATA_KEY, spend_data)

    except Exception as e:
        logger.error(f"Error occurred in spend_extractor: {str(e)}")


if __name__ == "__main__":
    spend_extractor()
