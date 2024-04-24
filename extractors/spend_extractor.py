"""
This function is responsible for loading and standardising Spend data.

Key Processes:
1. FileManager Initialisation:
   - Initialises FileManager for handling file operations.

2. Data Loading:
   - Loads the raw Spend data from the specified directory.
   - Assumes that the data is stored in an HTML file.

3. Data Standardisation:
   - Standardises the fields and structure of the loaded data.
   - Uses the DataStandardiser class to perform standardisation.
   - Optionally removes NaN values during standardisation.

4. Data Saving:
   - Saves the standardised Spend data to a designated location for further processing.

Usage:
- Executes as the main module to load and standardise Spend data.
- Prepares the data for subsequent analysis or processing steps.

Note:
- The script assumes that the data file is in a specific format and that the required directory structure exists.
- It's part of a larger data processing system for managing and analysing Spend data.
"""

# Import custom constants and utility functions
from constants import FileDirectory, Spend
from utility.file_manager import FileManager  # Import your FileManager class here
from utility.standardise_fields import DataStandardiser  # Custom data standardisation
from utility.logging import setup_logging  # Custom logging setup

# Initialise logging
logger = setup_logging()

#############################################################################################

def spend_extractor():
    """
    Load & Standardise Spend Data
    """
    # Initilaise FileManager Class
    file_manager = FileManager()

    # Load youtube HTML file from iCloud
    spend_data = file_manager.load_file(FileDirectory.MANUAL_EXPORT_PATH,
                                        Spend.RAW_DATA,  sheet_name=Spend.RAW_SHEET_NAME)

    # Standardise the dataframe fields
    standardiser = DataStandardiser()
    st_spend_data = standardiser.standardise_df(spend_data, remove_nans='Y')

    # Save Data
    file_manager.save_file(FileDirectory.RAW_DATA_PATH, st_spend_data, Spend.CLEAN_DATA)

if __name__ == "__main__":
    spend_extractor()

