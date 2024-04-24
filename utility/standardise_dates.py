# Import required libraries
from dateutil import parser  # For parsing date and time strings into datetime objects
import pandas as pd  # For data manipulation and analysis
import pytz  # For working with time zones

# Custom imports
from utility.logging import setup_logging  # Custom logging setup
from config import Settings

# Call the logging setup function to initialise logging
logger = setup_logging()

################################################################################

def convert_to_iso_with_tz(dt, local_tz=Settings.TIMEZONE):
    local_timezone = pytz.timezone(local_tz)

    try:
        if isinstance(dt, str):
            parsed_dt = parser.parse(dt)

            if parsed_dt.tzinfo is None or parsed_dt.tzinfo.utcoffset(parsed_dt) is None:
                parsed_dt = pytz.UTC.localize(parsed_dt)

            parsed_dt = parsed_dt.astimezone(local_timezone)
            parsed_dt = parsed_dt.replace(microsecond=0)
            return parsed_dt.isoformat()
        elif pd.isnull(dt):
            return None
        else:
            # Handle datetime objects
            if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                dt = pytz.UTC.localize(dt)
            dt = dt.astimezone(local_timezone)
            dt = dt.replace(microsecond=0)
            return dt.isoformat()
    except Exception as e:
        logger.error(f"Error converting datetime: {e}, with value: {dt}")
        return None

def standardise_dates(df, date_columns):
    """
    Standardize the date fields in a DataFrame to ISO 8601 format while keeping the original timezone offset.

    Parameters:
        df (pd.DataFrame): The DataFrame containing the date fields.
        date_columns (str, list): The name(s) of the column(s) containing the date fields. Can be a single string or a list of strings.

    Returns:
        pd.DataFrame: DataFrame with standardized date fields.
    """
    # Convert the date_columns parameter to a list if it's a single string
    if isinstance(date_columns, str):
        date_columns = [date_columns]

    # Loop through each date column to standardize it
    for date_column in date_columns:
        # Log initial data type and example values
        logger.info(f"Data type of {date_column} before conversion: {df[date_column].dtype}")
        logger.info(f"Two example values from {date_column} before conversion: {df[date_column].head(2).tolist()}")

        # Apply the conversion function to each element in the date column
        df[date_column] = df[date_column].apply(convert_to_iso_with_tz)

        # Log data type and example values after conversion
        logger.info(f"Data type of {date_column} after conversion: {df[date_column].dtype}")
        logger.info(f"The same two example values from {date_column} after conversion: {df[date_column].head(2).tolist()}")

    return df


