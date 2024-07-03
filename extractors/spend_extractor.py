import pandas as pd

from constants import FileDirectory, Spend
from utility import cache_data, standardise_data, upload_data_s3
from utility.file_manager import FileManager
from utility.logging import setup_logging

logger = setup_logging()


def spend_extractor():
    """
    Main function to load Spend data, drop NAs, standarise field names, cache to Redis, upload to S3, and
    save local copy.
    """
    logger.info("!!!!!!!!!!!! spend_extractor.py !!!!!!!!!!!")

    file_manager = FileManager()  # Initialise FileManager

    cache_key = "transactions_data"  # Define cache key

    cached_data = cache_data.get_cached_data(cache_key)  # Get cached data

    if cached_data is not None:  # Check if cached data exists
        spend_data = cached_data
        spend_data = pd.DataFrame(spend_data)  # Convert to DataFrame
        logger.info(f"Successfully fetched {len(spend_data)} total entries")
    else:
        spend_data = file_manager.load_file(
            FileDirectory.MANUAL_EXPORT_PATH,
            Spend.RAW_DATA,
            sheet_name=Spend.RAW_SHEET_NAME,
        )  # Load Spend data

        # Standardise data
        spend_data = standardise_data.CleanData.clean_data(spend_data, na_threshold=3)

        cache_data.update_cached_data(cache_key, spend_data)  # Cache new data
        # Upload data to S3, overwrite existing data
        upload_data_s3.post_data_to_s3(spend_data, cache_key, overwrite=True)
        file_manager.save_file(
            FileDirectory.RAW_DATA_PATH, spend_data, Spend.CLEAN_DATA
        )  # Save data to local file


if __name__ == "__main__":
    spend_extractor()
