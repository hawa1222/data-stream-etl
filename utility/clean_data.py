"""
Script for cleaning data by dropping NaN rows and standardising column names
"""

import re

from utility.log_manager import setup_logging

logger = setup_logging()


class CleanData:
    @staticmethod
    def drop_na_rows(df, threshold):
        """
        Keep rows with at least 'threshold' non-NaN values
        """
        rows_before = df.shape[0]
        df = df.dropna(thresh=threshold)  # Drop NaN rows
        rows_after = df.shape[0]

        logger.info(
            f"Dropped {rows_before - rows_after} rows with more than {threshold} NaN values"
        )

        return df

    @staticmethod
    def clean_col_names(col):
        """
        Standardise column names
        """
        original_col = col  # Store original column name for comparison
        col = col.strip()  # Remove leading/trailing whitespaces
        col = re.sub(r"\s+", "_", col)  # Replace whitespaces with underscores
        col = re.sub("(?<=[a-z])(?=[A-Z])", "_", col)  # + Underscore between camelCase

        return col.lower(), original_col != col.lower()

    def clean_data(df, na_threshold=None):
        """
        Drop NaN rows and standardise column names, with tracking changes

        Parameters:
            df: Original DataFrame
            na_threshold: Minimum non-NaN values required to keep a row (default: None)

        Returns:
            DataFrame: DataFrame with standardised column names and NaN rows removed
        """
        logger.info("Standardising field names and dropping NaN rows...")

        if na_threshold is not None:
            df = CleanData.drop_na_rows(df, threshold=na_threshold)

        df_fields = []  # Store all field names
        changed_fields = []  # Store changed field names
        for col in df.columns:
            new_col_name, changed = CleanData.clean_col_names(col)
            df_fields.append(new_col_name)
            if changed:
                changed_fields.append(new_col_name)

        df.columns = df_fields
        logger.info(f"{len(changed_fields)} field names changed: {changed_fields}")

        return df


def round_floats(df):
    """
    Rounds float fields in the DataFrame to 2 decimal places.

    Parameters:
        df: Input DataFrame.

    Returns:
        DataFrame: DataFrame with rounded float columns.
    """
    try:
        for column in df.select_dtypes(include=["float64"]).columns:
            df[column] = df[column].round(2)

        logger.info(
            f"Successfully rounded {len(df.select_dtypes(include=['float64']).columns)} float columns to 2 Decimal Places"
        )

        return df

    except Exception as e:
        logger.error(f"Error occurred in round_floats: {str(e)}")
        raise
