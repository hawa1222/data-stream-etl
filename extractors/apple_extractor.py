"""
This script is designed to extract and standardise data from Apple Health XML files.

Key Processes:
1. FileManager and DataStandardiser Initialisation:
   - Initialises FileManager for file operations and DataStandardiser for field standardisation.

2. Data Loading:
   - Loads the Apple Health XML file using FileManager.

3. XML Parsing:
   - Extracts the root element of the XML tree for data parsing.

4. Data Extraction and Standardisation:
   - Iterates over predefined elements of interest (like health records and activity elements).
   - Converts these elements to pandas DataFrames.
   - Applies standardisation to these DataFrames using DataStandardiser.

5. Data Filtering and Transformation:
   - Subsets and simplifies the 'type' field in the health records DataFrame.
   - Filters the DataFrame based on specific record types.

6. Data Saving:
   - Saves the processed and filtered data to specified file paths using FileManager.

Usage:
- Executes as the main module to process Apple Health XML data.
- Specifically targets the extraction and initial standardisation of data for later stages in a data processing pipeline.

Note:
- The script is part of a larger ETL process and focuses on extracting and preparing Apple Health data for further analysis.
"""

# Import Python system libraries
import pandas as pd

# Import custom constants and utility functions
from constants import FileDirectory, AppleHealth
from utility.file_manager import FileManager
from utility.standardise_fields import DataStandardiser  # Custom data standardisation
from utility.logging import setup_logging  # Custom logging setup

# Initialise logging
logger = setup_logging()

#############################################################################################

def apple_extractor():
    """
    Load & Standardise Apple Health Data
    """
    # Initialise FileManager and DataStandardiser classes
    file_manager = FileManager()
    standardiser = DataStandardiser()
   
    # Load Apple Health XML file
    apple_data_tree = file_manager.load_file(AppleHealth.APPLE_XML_PATH, AppleHealth.APPLE_XML_DATA)

    # Get the root element of the XML tree
    root = apple_data_tree.getroot()

    # Initialise an empty dictionary to store DataFrames
    dataframes_dict = {}

    # Define elements of interest
    elements_of_interest = [AppleHealth.RECORD, AppleHealth.ACTIVITY_ELEMENT]

    # Extract and standardise data for each element of interest
    for element in elements_of_interest:
        attributes = [elem.attrib for elem in root.iter(element)]
        # Check if attributes list is empty to avoid creating empty DataFrames
        if attributes:
            logger.info(f"Extracted {element} data from Apple Health XML file.")
            # Convert the list of dictionaries to a DataFrame and store it in the dictionary
            df = pd.DataFrame(attributes)
            df_standardised = standardiser.standardise_df(df)
            dataframes_dict[element] = df_standardised

    # Subset the DataFrame based on 'type' = AppleHealth.RECORD_TYPES
    record_df = dataframes_dict[AppleHealth.RECORD]
    # Simplify the 'type' by removing 'Identifier'
    record_df[AppleHealth.TYPE_FIELD] = record_df[AppleHealth.TYPE_FIELD].str.split('Identifier').str[-1]
    filtered_record_df = record_df[record_df[AppleHealth.TYPE_FIELD].isin(AppleHealth.RECORD_ELEMENTS)]

    # Save Data
    file_manager.save_file(FileDirectory.RAW_DATA_PATH, filtered_record_df, AppleHealth.RECORD_DATA)
    #file_manager.save_file(FileDirectory.RAW_DATA_PATH, df_part2, AppleHealth.RECORD_DATA_2)
    file_manager.save_file(FileDirectory.RAW_DATA_PATH, dataframes_dict[AppleHealth.ACTIVITY_ELEMENT], AppleHealth.ACTIVITY_DATA)

if __name__ == "__main__":
    apple_extractor()

