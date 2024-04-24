"""
This script is designed to load Daylio data into a database by creating and populating tables.

Key Processes:
1. FileManager and DatabaseHandler Initialisation:
   - Initialises FileManager for handling file operations and DatabaseHandler for database interactions.

2. Dataset Definition:
   - Defines Daylio datasets, including mood data, activity list data, and activity data.
   - Specifies the corresponding table fields (schema) for each dataset.

3. Data Loading, Table Creation, and Data Insertion:
   - Iterates through each dataset:
     - Loads data from the corresponding file into a pandas DataFrame.
     - Creates a database table for the dataset with the defined schema.
     - Inserts the data from the DataFrame into the newly created table.

4. Connection Closure:
   - Closes the database connection after all operations are completed.

Usage:
- Executes as the main module to handle the loading of Daylio data into a relational database.
- Ensures that Daylio data is structured and stored for further analysis or reporting.

Note:
- The script assumes that the data files are in a specific format and that the database does not contain the tables beforehand.
- It's part of a larger data processing system for managing and analysing Daylio data.
"""

# Custom imports
from constants import FileDirectory, Daylio
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from utility.file_manager import FileManager
from utility.logging import setup_logging
from utility.database_handler import DatabaseHandler

# Initialise logging
logger = setup_logging()

##################################################################################################################################

def daylio_loader():
    # Initialise FileManager Class
    file_manager = FileManager()

    # Define Daylio datasets and corresponding table fields
    datasets = {
        Daylio.MOOD_DATA.split('.')[0]: (Daylio.MOOD_DATA, {
            'date_time': 'TIMESTAMP NOT NULL PRIMARY KEY',
            'mood': 'VARCHAR(20) NOT NULL',
            'note_title': 'TEXT',
            'note': 'TEXT'
        }),
        Daylio.ACTIVITY_DATA.split('.')[0]: (Daylio.ACTIVITY_DATA, {
            'id': 'SMALLINT UNSIGNED NOT NULL PRIMARY KEY',
            'date_time': 'TIMESTAMP NOT NULL, FOREIGN KEY (date_time) REFERENCES daylio_mood(date_time)',
            'activities': 'VARCHAR(50)'
        })
    }
    
    # Initialise the DatabaseHandler
    db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

    # Load data, create tables, and insert data for each dataset
    # table_name refers to key in datasets dictionary, api_data refers to the file name, fields refers to the schema
    for table_name, (api_data, fields) in datasets.items():
        df = file_manager.load_file(FileDirectory.CLEAN_DATA_PATH, api_data)
        db_handler.create_table(table_name, fields)
        db_handler.insert_data(table_name, df, list(fields.keys()))
   
    # Close the database connection
    db_handler.close_connection()


if __name__ == '__main__':
    daylio_loader()
