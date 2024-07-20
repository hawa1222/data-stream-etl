import pandas as pd

from config import Settings
from constants import FileDirectory, Spend
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def clean_date_column(df):
    """
    Cleans date column in DataFrame.
    """
    logger.debug(f"Cleaning '{Spend.DATE}' field...")

    logger.debug(f"Sample before cleaning: {df[Spend.DATE].head(2).to_list()}")
    df[Spend.DATE] = pd.to_datetime(df[Spend.DATE]).dt.strftime(Settings.DATE_FORMAT)
    logger.debug(f"Sample after cleaning: {df[Spend.DATE].head(2).to_list()}")

    return df


def spend_transformer():
    """
    Main function to load Spend Data, clean & transform it, and save to local storage.
    """

    logger.info("!!!!!!!!!!!! spend_transformer.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()

        df = file_manager.load_file(FileDirectory.RAW_DATA_PATH, Spend.DATA_KEY)

        clean_date_column(df)  # Clean 'date' column

        file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, Spend.DATA_KEY, df)

    except Exception as e:
        logger.error(f"Error occurred in spend_transformer: {str(e)}")
        raise


if __name__ == "__main__":
    spend_transformer()
