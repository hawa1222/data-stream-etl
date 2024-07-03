from constants import Daylio, FileDirectory
from utility import cache_data, standardise_data, upload_data_s3
from utility.file_manager import FileManager
from utility.logging import setup_logging

logger = setup_logging()


def daylio_extractor():
    """
    Main function to load Daylio data, cache to Redis, upload to S3, and
    save local copy.
    """
    logger.info("!!!!!!!!!!!! daylio_extractor.py !!!!!!!!!!!")

    file_manager = FileManager()  # Initialise FileManager

    cache_key = "daylio_data"  # Define cache key

    cached_data = cache_data.get_cached_data(cache_key)  # Get cached data

    if cached_data is not None:  # Check if cached data exists
        logger.info(f"Successfully fetched {len(cached_data)} total entries")
    else:
        # Load youtube HTML file from iCloud
        daylio_data = file_manager.load_file(
            FileDirectory.MANUAL_EXPORT_PATH, Daylio.RAW_DATA
        )  # Load Daylio data

        # Standardise data
        daylio_data = standardise_data.CleanData.clean_data(daylio_data, na_threshold=5)

        cache_data.update_cached_data(cache_key, daylio_data)  # Cache new data
        # Upload data to S3, overwrite existing data
        upload_data_s3.post_data_to_s3(daylio_data, cache_key, overwrite=True)
        file_manager.save_file(
            FileDirectory.RAW_DATA_PATH, daylio_data, Daylio.CLEAN_DATA
        )  # Save data to local file


if __name__ == "__main__":
    daylio_extractor()
