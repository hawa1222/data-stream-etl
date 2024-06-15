import os
import warnings

import pandas as pd

from utility.file_manager import FileManager
from utility.logging import setup_logging

logger = setup_logging()


def update_local_data(file_directory, file_name, new_data):
    logger.info(f"Updating local copy of '{file_name}'...")

    if new_data.empty:
        logger.warning("New dataframe is empty. Skipping upload to local disk")
        return False

    file_manager = FileManager()

    local_data_path = os.path.join(file_directory, file_name)

    if os.path.exists(local_data_path):
        existing_data = file_manager.load_file(file_directory, file_name)
        logger.info(
            f"Local copy loaded. Existing data shape: {existing_data.shape}, New data shape: {new_data.shape}"
        )
    else:
        existing_data = pd.DataFrame()
        logger.info("Local copy doesn't exist, empty dataframe created")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=FutureWarning)
        combined_data = pd.concat(
            [existing_data, new_data], join="outer", ignore_index=True
        )
        logger.info(
            f"Local copy updated, and saved to disk. Updated data shape: {combined_data.shape}"
        )

    # Save combined data to Excel
    file_manager.save_file(file_directory, combined_data, file_name)


def initialise_cache(file_directory, file_name):
    """
    Initialise the cache by either loading an existing file or creating a new one empty dataframe

    Parameters:
        file_directory: Directory for where to load file from.
        file_name: The name of the cache file.

    Returns:
        cached_data: Empty dataframe or with cached data.
    """

    file_manager = FileManager()

    cache_file = os.path.join(file_directory, file_name)
    if os.path.exists(cache_file):
        cached_data = file_manager.load_file(file_directory, file_name)
        logger.info("Cached data loaded")
    else:
        cached_data = pd.DataFrame()
        logger.info("Cached data created")
    return cached_data


def update_cache(file_directory, cached_data, new_data, file_name, id_col):
    """
    Update the cache with new data.

    Parameters:
        file_directory: Directory for where to save file to.
        cached_data: Existing dataframe containing cached data.
        new_data: Dataframe containing new data.
        file_name: Cache file name.
        id_col: Name of the column containing IDs in new_data.

    Returns:
        Dataframe with updated cached data.
    """

    file_manager = FileManager()

    # If the cache is empty or does not exist, write the entire new_data to it
    if cached_data.empty:
        file_manager.save_file(file_directory, new_data, file_name)
        logger.info("Cached data is empty, new data saved")
        return new_data

    # Check if new_data is empty; if so, return the existing cached_data
    if new_data.empty:
        logger.info("New data is empty, no update needed")
        return cached_data

    # Create sets of IDs for existing cached_data and new_data
    cached_data_ids = set(cached_data[id_col])
    new_data_ids = set(new_data[id_col])

    # Find new IDs that are not in the cache
    new_ids = new_data_ids - cached_data_ids

    # Filter rows with new IDs from new_data
    rows_to_add = new_data[new_data[id_col].isin(new_ids)]

    # Append these new rows to the cached_data
    updated_cached_data = pd.concat([cached_data, rows_to_add], ignore_index=True)

    # Save updated cached data to a file
    file_manager.save_file(file_directory, updated_cached_data, file_name)

    logger.info("Cached data updated and saved")

    return updated_cached_data
