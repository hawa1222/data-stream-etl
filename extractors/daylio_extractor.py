"""
This script facilitates the extraction and initial processing of Daylio data.

Key Processes:
1. FileManager Initialisation:
   - Creates an instance of the FileManager class to handle file operations.

2. Data Loading:
   - Loads the Daylio data file (possibly in a raw format) from a specified manual export directory using FileManager.

3. Data Saving:
   - Saves the loaded Daylio data into a designated directory for raw data, setting it up for future processing stages.

Usage:
- Designed to be run as the main module, the script invokes the `daylio_extractor` function to extract Daylio data.
- It's tailored specifically for the initial stage of handling Daylio data, focusing on its extraction and storage in a raw data format.

Note:
- The script is a part of an ETL (Extract, Transform, Load) pipeline, primarily focusing on the 'Extract' phase. 
- As of its current state, the script does not include data transformation or standardisation processes.
"""

# =============================================================================
# # Import Python system libraries
# import sys  # For Python interpreter control
# # Configuration
# sys.dont_write_bytecode = True  # Prevent Python from writing bytecode files (.pyc)
# sys.path.append('/Users/hadid/GitHub/ETL')  # Add path to system path
# =============================================================================

# Import custom constants and utility functions
from constants import FileDirectory, Daylio
from utility.file_manager import FileManager
from utility.logging import setup_logging  # Custom logging setup

# Initialise logging
setup_logging()

#############################################################################################

def daylio_extractor():
    """
    Load & Standardise Daylio Data
    """
    # Initilaise FileManager Class
    file_manager = FileManager()

    # Load youtube HTML file from iCloud
    daylio_data = file_manager.load_file(FileDirectory.MANUAL_EXPORT_PATH, Daylio.RAW_DATA)

    # Save Data
    file_manager.save_file(FileDirectory.RAW_DATA_PATH, daylio_data, Daylio.CLEAN_DATA)


if __name__ == "__main__":
    daylio_extractor()
