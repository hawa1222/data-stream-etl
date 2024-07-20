import pandas as pd

from constants import Apple, FileDirectory
from utility import redis_manager
from utility.clean_data import CleanData
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def subset_record_by_type(df, type_column="type", max_rows=10000):
    """
    Filter dataframe by specific column and sample if number of rows exceeds max_rows.

    Parameters:
        df: Dataframe to filter.
        type_column: Column to filter by.
        max_rows: Maximum number of rows to keep for each unique type.

    Returns:
        DataFrame: Filtered dataframe.
    """
    logger.debug(f"Reducing 'apple_records' data to {max_rows} rows per type...")

    logger.debug(f"Original data has {len(df)} rows")

    result_df = (
        df.groupby(type_column)
        .apply(lambda x: x.sample(n=min(len(x), max_rows), random_state=42))
        .reset_index(drop=True)
    )

    logger.debug(f"Filtered data has {len(result_df)} rows")

    return result_df


def extract_xml_data(root):
    """
    Extracts data from Apple Health XML file.

    Parameters:
        root (Element): Root element of XML file.

    Returns:
        dict: Dictionary containing the extracted dataframes, where keys are element names and values are corresponding dataframes.
    """
    logger.debug("Extracting data from Apple Health XML file...")

    dataframes_dict = {}
    for element in [Apple.RECORD_ELEMENT, Apple.ACTIVITY_ELEMENT]:
        elem_data = [elem.attrib for elem in root.iter(element)]
        if elem_data:
            logger.info(f"Successfully extracted '{element}' element")
            df = pd.DataFrame(elem_data)
            logger.debug(f"Fields in '{element}' dataframe:\n\n{df.columns.tolist()}\n")
            df_standardised = CleanData.clean_data(df, 4)
            dataframes_dict[element] = df_standardised

    return dataframes_dict


def apple_extractor():
    """
    Main function to load Apple Health XML data, extract and standardise data,
    update cache, and save to local storage.
    """
    logger.info("!!!!!!!!!!!! apple_extractor.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()

        record_cache = redis_manager.get_cached_data(Apple.RECORD_DATA)

        if record_cache is not None:
            activity_cache = redis_manager.get_cached_data(Apple.ACTIVITY_DATA)
            filtered_record_df = pd.DataFrame(record_cache)
            activity_df = pd.DataFrame(activity_cache)
            logger.info(
                f"Successfully fetched {len(filtered_record_df)} & {len(activity_df)} entries from {Apple.RECORD_DATA} & {Apple.ACTIVITY_DATA} cache respectively"
            )
        else:
            tree = file_manager.load_file(FileDirectory.SOURCE_DATA_PATH, Apple.XML_DATA, "xml")
            root = tree.getroot()

            dataframes_dict = extract_xml_data(root)

            record_df = dataframes_dict[Apple.RECORD_ELEMENT]
            activity_df = dataframes_dict[Apple.ACTIVITY_ELEMENT]

            record_df[Apple.TYPE_FIELD] = (
                record_df[Apple.TYPE_FIELD].str.split("Identifier").str[-1]
            )  # Keep text after 'Identifier' in 'type' field
            filtered_record_df = record_df[
                record_df[Apple.TYPE_FIELD].isin(Apple.RECORD_TYPE)
            ]  # Filter records by type
            unique_categories = filtered_record_df["type"].value_counts()
            type_counts = {category: count for category, count in unique_categories.items()}
            formatted_type_counts = "\n".join(
                [f"{category}: {count}" for category, count in type_counts.items()]
            )
            logger.debug(f"Unique record types:\n\n{formatted_type_counts}\n")

            # Update cache and save to local storage
            subset_record_df = subset_record_by_type(filtered_record_df)
            redis_manager.update_cached_data(Apple.RECORD_DATA, subset_record_df)
            redis_manager.update_cached_data(Apple.ACTIVITY_DATA, activity_df)

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
