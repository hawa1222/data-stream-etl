"""
Script to clean and standardise date strings.
"""

import re
from datetime import datetime

from zoneinfo import ZoneInfo

from utility.log_manager import setup_logging

logger = setup_logging()


def parse_date(date_string):
    """
    Parses a date string and converts it to UTC format.

    Args:
        date_string: Date string to be parsed.

    Returns:
        str: Parsed date string in UTC format.

    Raises:
        ValueError: If the date string cannot be parsed.

    Logic:
        1. Strip leading and trailing spaces from the date string.
        2. Extract timezone information from the date string.
        3. Remove timezone information from the original string.
        4. Define standard timezone offsets.
        5. Convert timezone to offset if necessary.
        6. Define possible date formats (without timezone info).
        7. Try parsing the date string with each format.
        8. If a format matches, create timezone info and convert to UTC.
        9. Return the parsed date string in UTC format.
        10. If no format matches, raise a ValueError.

    Example:
        >>> parse_date("2024-03-29T15:04:46")
        '2024-03-29T15:04:46Z'
    """

    date_string = str(date_string).strip()

    # Extract timezone information
    tz_match = re.search(r"(Z|GMT|BST|UTC|[+-]\d{4})", date_string)
    tz_info = tz_match.group(1) if tz_match else None

    # Remove timezone information from the original string
    date_string = re.sub(r"(Z|GMT|BST|UTC|[+-]\d{4})", "", date_string).strip()

    # Define standard timezone offsets
    tz_offsets = {"Z": "+0000", "GMT": "+0000", "UTC": "+0000", "BST": "+0100"}

    # Convert timezone to offset if necessary
    if tz_info in tz_offsets:
        tz_offset = tz_offsets[tz_info]
    elif tz_info and (tz_info.startswith("+") or tz_info.startswith("-")):
        tz_offset = tz_info
    else:
        tz_offset = "+0000"  # Default to UTC if no timezone info

    formats = [
        "%Y-%m-%dT%H:%M:%S",  # 2024-03-29T15:04:46
        "%Y-%m-%d %I:%M%p",  # 2024-03-29 3:04PM
        "%b %d, %Y, %I:%M:%S %p",  # Mar 29, 2024, 3:04:46 PM
        "%Y-%m-%d %H:%M:%S",  # 2024-03-29 15:04:46
        "%Y-%m-%dT%H:%M:%S.%f",  # 2024-03-29T15:04:46.123456
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_string, fmt)

            # Create timezone info
            offset_hours, offset_minutes = int(tz_offset[1:3]), int(tz_offset[3:5])
            total_offset = offset_hours * 60 + offset_minutes
            if tz_offset.startswith("-"):
                total_offset = -total_offset
            tz = ZoneInfo(
                f"Etc/GMT{'-' if total_offset >= 0 else '+'}{abs(total_offset) // 60}"
            )

            # Apply timezone and convert to UTC
            dt = dt.replace(tzinfo=tz)
            utc_dt = dt.astimezone(ZoneInfo("UTC"))

            return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        except ValueError:
            continue

    raise ValueError(f"Unable to parse datetime string: {date_string}")


####################################################################################################

import logging

import pandas as pd


def parse_date(date_string, output_format="%Y-%m-%d %H:%M:%S"):
    """
    Parses date string and converts it to specified format (default: UTC format).

    Parameters:
    - date_string (str): Date string to be parsed.
    - output_format (str): The desired format of output date string (default: '%Y-%m-%d %H:%M:%S').

    Returns:
    - str: Parsed date string in specified output format.

    Raises:
    - ValueError: If date string cannot be parsed.

    Logic:
    1. Handles None, NaN, and other non-date values by returning None.
    2. Strips leading and trailing spaces from date string.
    3. Defines possible date formats (without timezone info).
    4. Tries parsing date string with each format.
    5. Returns parsed date string in specified format.
    6. If no format matches, returns None to gracefully handle unknown formats.
    """

    if pd.isna(date_string) or date_string is None:
        return None

    date_string = str(date_string).strip()

    # Define your date formats (assumed from your code as 'Migration.SOURCE_DATETIME_FMTS')
    possible_formats = [
        "%Y-%m-%dT%H:%M:%S",  # Add other formats you expect
        "%Y-%m-%d %H:%M:%S",
        "%d-%b-%y",  # Example: '22-Oct-84'
        "%Y/%m/%d",
        "%d/%m/%Y",
    ]

    for fmt in possible_formats:
        try:
            dt = datetime.strptime(date_string, fmt)
            return dt.strftime(output_format)
        except ValueError:
            continue

    # If no valid format is found, return None
    logging.warning(f"Unable to parse datetime string: {date_string}")
    return None


def standardise_dates(df, date_fields, output_format="%Y-%m-%d %H:%M:%S"):
    """
    Standardises date fields in a dataframe by applying the `parse_date` function to each specified field.

    Parameters:
    - df (pd.DataFrame): The dataframe containing the date fields.
    - date_fields (list): List of columns in the dataframe that contain date values.
    - output_format (str): The format to convert the dates to (default: '%Y-%m-%d %H:%M:%S').

    Returns:
    - pd.DataFrame: The dataframe with the standardised date fields.
    """

    logging.info(f"Standardising date fields {date_fields}...")

    for col in date_fields:
        logging.info(f"Sample before cleaning: {col} -- {df[col].head(2).to_list()}")
        df[col] = df[col].astype(str).apply(lambda x: parse_date(x, output_format))
        logging.info(f"Sample after cleaning: {col} -- {df[col].head(2).to_list()}")

    return df
