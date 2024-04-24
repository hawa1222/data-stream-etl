# Import required libraries
import logging  # For logging information and debugging
import os
from logging.handlers import RotatingFileHandler

# Custom imports
from config import Settings
from constants import FileDirectory

# ------------------------------
# Logging Configuration
# ------------------------------

def setup_logging():
    """
    Set up logging configuration.

    This function creates a logger, sets the logging level, and adds console and file handlers to the logger.

    Returns:
        logger (logging.Logger): The configured logger object.
    """
    
    # Create a logger
    logger = logging.getLogger(__name__)
    # Prevent logs from propagating to the parent logger
    logger.propagate = False
    
    
    # Remove existing handlers, if any
    '''
    [:] is used to create a copy of the list of handlers
    By iterating over a copy ([:]), the original list's indices are effectively disconnected 
    from the loop's progress.
    '''
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Set the logging level
    logging_level = getattr(logging, Settings.LOGGING_LEVEL.upper(), logging.INFO)
    logger.setLevel(logging_level)

    # Create a formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Create a file handler with log rotation
    log_directory = os.path.join(FileDirectory.ROOT_DIRECTORY, 'logs')
    os.makedirs(log_directory, exist_ok=True)
    log_file = os.path.join(log_directory, 'logs.txt')
    file_handler = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=10)
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger