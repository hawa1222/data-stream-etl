import pandas as pd

from constants import Daylio, FileDirectory
from utility import redis_manager, s3_manager
from utility.clean_data import CleanData
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def daylio_extractor():
    """
    Main function to load Daylio data, drop NAs, standarise field names, update cache, save to S3 and local storage.
    """
    logger.info("!!!!!!!!!!!! daylio_extractor.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()

        cached_data = redis_manager.get_cached_data(Daylio.DATA_KEY)

        if cached_data is not None:  # Check cached data exists
            daylio_df = pd.DataFrame(cached_data)  # Convert to df
            logger.info(f"Successfully fetched {len(daylio_df)} total entries")
        else:
            daylio_df = file_manager.load_file(
                FileDirectory.SOURCE_DATA_PATH, Daylio.DATA_KEY, "csv"
            )

            daylio_df = CleanData.clean_data(daylio_df, 5)

            # Cache new data, upload to S3
            redis_manager.update_cached_data(Daylio.DATA_KEY, daylio_df)
            s3_manager.post_data_to_s3(Daylio.DATA_KEY, daylio_df, overwrite=True)

        file_manager.save_file(FileDirectory.RAW_DATA_PATH, Daylio.DATA_KEY, daylio_df)

    except Exception as e:
        logger.error(f"Error occurred in daylio_extractor: {str(e)}")


if __name__ == "__main__":
    daylio_extractor()
