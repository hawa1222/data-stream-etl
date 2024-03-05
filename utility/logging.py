# Import required libraries
import logging  # For logging information and debugging
import os

# =============================================================================
# import sys
# # Add the path to the directory containing utils.py to sys.path
# sys.dont_write_bytecode = True
# sys.path.append('/Users/hadid/GitHub/ETL')  # Add path to system path
# =============================================================================

# Custom imports
from config import Settings
from constants import FileDirectory

#############################################################################################

# Define the setup_logging function to configure logging settings
def setup_logging():
    """
    Sets up the logging configuration.

    Raises:
    ValueError: If the specified logging level in the configuration is invalid.

    Returns:
    None
    """

     # Get the root logger
    logger = logging.getLogger()
    
    # Check if the logger already has handlers attached, and if so, return early.
    # This prevents adding multiple handlers to the logger, which causes repeated log messages.
    if logger.hasHandlers():
        return

    # Get the logging level from the Settings
    logging_level = getattr(logging, Settings.LOGGING_LEVEL.upper(), None)

    # Validate the logging level
    if not isinstance(logging_level, int):
        raise ValueError(f'Invalid log level: {Settings.LOGGING_LEVEL}')

    # Configure the basic settings for logging
    logging.basicConfig(level=logging_level, format='%(levelname)s - %(message)s')

    # Create a file handler to write logs to a file
    file_handler = logging.FileHandler(os.path.join(FileDirectory.ROOT_DIRECTORY, 'log.txt'))
    file_handler.setLevel(logging_level)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # Add the file handler to the logger
    logger.addHandler(file_handler)