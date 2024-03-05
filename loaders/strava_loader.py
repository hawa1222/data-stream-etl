"""
This script is responsible for loading Strava data into a relational database by creating and populating tables.

Key Processes:
1. FileManager and DatabaseHandler Initialisation:
   - Initialises FileManager for handling file operations and DatabaseHandler for database interactions.

2. Dataset Definition:
   - Defines Strava datasets, including performance data, sport type data, gear data, and activity data.
   - Specifies the corresponding table fields (schema) for each dataset.

3. Data Loading, Table Creation, and Data Insertion:
   - Iterates through each dataset:
     - Loads data from the corresponding Excel file into a pandas DataFrame.
     - Creates a database table for the dataset with the defined schema.
     - Inserts the data from the DataFrame into the newly created table.

4. Connection Closure:
   - Closes the database connection after all operations are completed.

Usage:
- Execute this script as the main module to handle the loading of Strava data into a relational database.
- Ensure that Strava data files are available in the specified directory.
- The script assumes that the database tables do not exist beforehand.

Note:
- This script is part of a larger data processing system for managing Strava data.
- It ensures that Strava data is structured and stored in a relational database for further analysis or reporting.
"""


# =============================================================================
# # Import the required libraries
# import sys
# # Configuration
# sys.dont_write_bytecode = True  # Prevent Python from writing bytecode files (.pyc)
# sys.path.append('/Users/hadid/GitHub/ETL')  # Add path to system path
# =============================================================================

# Custom imports
from constants import FileDirectory, StravaAPI
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from utility.file_manager import FileManager
from utility.logging import setup_logging
from utility.database_handler import DatabaseHandler

# Initialise logging
setup_logging()

##################################################################################################################################

def strava_loader():
    # Initialise FileManager Class
    file_manager = FileManager()

    # Define Strava datasets and corresponding table fields
    datasets = {
        StravaAPI.PERFORMANCE_DATA.split('.')[0]: (StravaAPI.PERFORMANCE_DATA, {
            'activity_id': 'BIGINT UNSIGNED NOT NULL PRIMARY KEY',
            'distance': 'DECIMAL(6,1)',
            'moving_time': 'DECIMAL(5,2)',
            'elapsed_time': 'DECIMAL(5,2)',
            'average_speed': 'DECIMAL(4,3)',
            'max_speed': 'DECIMAL(5,3)',
            'average_cadence': 'DECIMAL(4,1)',
            'average_heartrate': 'DECIMAL(4,1)',
            'max_heartrate': 'TINYINT UNSIGNED',
            'calories': 'DECIMAL(5,1)',
            'suffer_score': 'TINYINT UNSIGNED'
        }),
        StravaAPI.SPORT_DATA.split('.')[0]: (StravaAPI.SPORT_DATA, {
            'sport_type_id': 'TINYINT UNSIGNED NOT NULL PRIMARY KEY',
            'sport_type': 'VARCHAR(50)'
        }),
        StravaAPI.GEAR_DATA.split('.')[0]: (StravaAPI.GEAR_DATA, {
            'gear_id': 'TINYINT UNSIGNED NOT NULL PRIMARY KEY',
            'gear_name': 'VARCHAR(50)'
        }),
        StravaAPI.ACTIVITY_DATA.split('.')[0]: (StravaAPI.ACTIVITY_DATA, {
            'activity_id': 'BIGINT UNSIGNED NOT NULL PRIMARY KEY',
            'external_id': 'VARCHAR(100)',
            'device_name': 'VARCHAR(50)',
            'name': 'VARCHAR(50)',
            'sport_type_id': 'TINYINT UNSIGNED NOT NULL, FOREIGN KEY (sport_type_id) REFERENCES strava_sport_type(sport_type_id)',
            'start_date': 'TIMESTAMP',
            'gear_id': 'TINYINT UNSIGNED NULL, FOREIGN KEY (gear_id) REFERENCES strava_gear(gear_id)',
            'private_note': 'VARCHAR(100)',
            'polyline': 'TEXT'
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
    strava_loader()
