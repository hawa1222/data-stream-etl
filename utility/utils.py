# Import the required libraries
import inspect
import os  # For operating system related functionality
import traceback
from datetime import datetime

from config import Settings
from utility.logging import setup_logging

# Setting up logging
logger = setup_logging()

#############################################################################################


def exception_formatter(e, library_name=None):
    """
    Formats exception message and traceback for logging purposes.

    Args:
        e (Exception): Exception object.
        library_name (str, optional): Mame of the library that raised the exception. Defaults to None.

    Returns:
        str: Formatted error message.
    """

    exception_type = type(e).__name__
    exception_message = str(e)
    trace = traceback.format_exc()

    if library_name == "requests":
        error_message = f"{exception_type} {e.response.status_code} occurred... \n\n {e.response.text}\n\n"
        error_message += trace

        return error_message

    error_message = f"{exception_type} occurred... \n\n {exception_message}\n\n"
    error_message += trace

    return error_message


def format_data_name(data_name):
    """
    Formats the data file name by removing the file extension and the prefix based on the calling script name.

    Parameters:
        data_name (str): The name of the data file.

    Returns:
        str: The formatted data key.
    """
    # Remove file extension
    formatted_name = data_name.split(".")[0]

    # Get the calling frame
    calling_frame = inspect.currentframe().f_back

    # Get the name of the calling script
    calling_script_name = os.path.basename(calling_frame.f_globals["__file__"])

    # Extract the prefix from the script name up to the first underscore
    prefix = calling_script_name.split("_")[0] + "_"

    # Remove the prefix from the formatted name, if it exists
    if formatted_name.startswith(prefix):
        formatted_name = formatted_name[len(prefix) :]

    return formatted_name


def generate_filename(base_filename):
    """
    Generates a filename with a timestamp.

    Parameters:
    base_filename (str): The base filename.

    Returns:
    str: The new filename with a timestamp.
    """
    timestamp = datetime.now().strftime(Settings.TIMESTAMP)
    return base_filename.replace(".xlsx", f"_{timestamp}.xlsx")
