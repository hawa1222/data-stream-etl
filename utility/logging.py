import logging
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

    This function creates logger, sets logging level,
    and adds console and file handlers to logger.

    Returns:
        logger (logging.Logger): configured logger object.
    """

    # Create logger
    logger = logging.getLogger(__name__)
    # Prevent logs from propagating to parent logger
    logger.propagate = False

    # Remove existing handlers, if any.[:] is used to create copy of list of handlers
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    # Set logging level
    logging_level = getattr(logging, Settings.LOGGING_LEVEL.upper(), logging.INFO)
    logger.setLevel(logging_level)

    # Create formatter
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s]  %(message)s", datefmt=Settings.DATETIME_FORMAT
    )

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Create file handler with log rotation
    log_directory = os.path.join(FileDirectory.ROOT_DIRECTORY, "logs")
    os.makedirs(log_directory, exist_ok=True)  # Create logs directory
    log_file = os.path.join(log_directory, "logs.txt")  # Log file path
    file_handler = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=10)
    file_handler.setFormatter(formatter)  # Set formatter

    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
