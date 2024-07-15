import pandas as pd

from constants import Apple, FileDirectory
from utility.clean_data import CleanData
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def apple_extractor():
    """
    Main function to load Apple Health XML data, extract and standardise data,
    and save to local storage.
    """
    try:
        logger.info("!!!!!!!!!!!! apple_extractor.py !!!!!!!!!!!")

        file_manager = FileManager()

        tree = file_manager.load_file(
            FileDirectory.SOURCE_DATA_PATH, Apple.XML_DATA, "xml"
        )  # Load Apple Health XML file

        root = tree.getroot()  # Get root of XML tree

        dataframes_dict = {}

        # Extract and standardise data for each element of interest
        logger.info("Extracting data from Apple Health XML file...")
        for element in [Apple.RECORD_ELEMENT, Apple.ACTIVITY_ELEMENT]:
            elem_data = [elem.attrib for elem in root.iter(element)]
            if elem_data:
                df = pd.DataFrame(elem_data)
                logger.info(f"Fields in {element} DataFrame: {df.columns.tolist()}")
                df_standardised = CleanData.clean_data(df, 4)
                dataframes_dict[element] = df_standardised
                logger.info(f"Succesfully extracted {element} data")

        record_df = dataframes_dict[Apple.RECORD_ELEMENT]  # Extract record df
        activity_df = dataframes_dict[Apple.ACTIVITY_ELEMENT]  # Extract activity df

        record_df[Apple.TYPE_FIELD] = (
            record_df[Apple.TYPE_FIELD].str.split("Identifier").str[-1]
        )  # Keep text after 'Identifier' in 'type' field
        filtered_record_df = record_df[
            record_df[Apple.TYPE_FIELD].isin(Apple.RECORD_TYPE)
        ]  # Filter records by type
        logger.info(
            f"Unique record types: {filtered_record_df[Apple.TYPE_FIELD].unique()}"
        )

        file_manager.save_file(
            FileDirectory.RAW_DATA_PATH,
            Apple.RECORD_DATA,
            filtered_record_df,
            extension="csv",
        )
        file_manager.save_file(
            FileDirectory.RAW_DATA_PATH,
            Apple.ACTIVITY_DATA,
            activity_df,
            extension="csv",
        )

    except Exception as e:
        logger.error(f"Error occurred in apple_extractor: {str(e)}")


if __name__ == "__main__":
    apple_extractor()
