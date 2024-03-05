"""
This script is responsible for loading YouTube data into a relational database by creating and populating tables.

Key Processes:
1. FileManager and DatabaseHandler Initialisation:
   - Initialises FileManager for handling file operations and DatabaseHandler for database interactions.

2. Dataset Definition:
   - Defines YouTube datasets, including clean playlist data and subscriptions data.
   - Specifies the corresponding table fields (schema) for each dataset.

3. Data Loading, Table Creation, and Data Insertion:
   - Iterates through each dataset:
     - Loads data from the corresponding Excel file into a pandas DataFrame.
     - Creates a database table for the dataset with the defined schema.
     - Inserts the data from the DataFrame into the newly created table.

4. Connection Closure:
   - Closes the database connection after all operations are completed.

Usage:
- Execute this script as the main module to handle the loading of YouTube data into a relational database.
- Ensure that YouTube data files are available in the specified directory.
- The script assumes that the database tables do not exist beforehand.

Note:
- This script is part of a larger data processing system for managing YouTube data.
- It ensures that YouTube data is structured and stored in a relational database for further analysis or reporting.
"""

# =============================================================================
# # Import the required libraries
# import sys
# # Configuration
# sys.dont_write_bytecode = True  # Prevent Python from writing bytecode files (.pyc)
# sys.path.append('/Users/hadid/GitHub/ETL')  # Add path to system path
# =============================================================================

# Custom imports
from constants import FileDirectory, Youtube
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from utility.file_manager import FileManager
from utility.logging import setup_logging
from utility.database_handler import DatabaseHandler

# Initialise logging
setup_logging()

##################################################################################################################################

def youtube_loader():
    # Initialise FileManager Class
    file_manager = FileManager()

    # Define YouTube datasets and corresponding table fields
    datasets = {
        Youtube.CLEAN_PLAYLIST_DATA.split('.')[0]: (Youtube.CLEAN_PLAYLIST_DATA, {
            'published_at': 'TIMESTAMP NOT NULL PRIMARY KEY',
            'playlist': 'VARCHAR(10)',
            'source': 'VARCHAR(10)',
            'video_id': 'VARCHAR(20)',
            'title': 'TEXT',
            'description': 'TEXT',
            'thumbnail_url': 'VARCHAR(255)',
            'video_owner_channel_title': 'VARCHAR(60)',
            'channel_id': 'VARCHAR(30)'
        }),
        Youtube.SUBS_DATA.split('.')[0]: (Youtube.SUBS_DATA, {
            'published_at': 'TIMESTAMP NOT NULL PRIMARY KEY',
            'resource_id_channel_id': 'VARCHAR(60)',
            'title': 'TEXT',
            'description': 'TEXT',
            'thumbnail_url': 'VARCHAR(255)'
        })
    }

    # Initialise the DatabaseHandler
    db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

    # Load data, create tables, and insert data for each dataset
    for table_name, (api_data, fields) in datasets.items():
        df = file_manager.load_file(FileDirectory.CLEAN_DATA_PATH, api_data)
        db_handler.create_table(table_name, fields)
        db_handler.insert_data(table_name, df, list(fields.keys()))

    # Close the database connection
    db_handler.close_connection()


if __name__ == '__main__':
    youtube_loader()
