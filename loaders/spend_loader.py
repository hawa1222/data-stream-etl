"""
This script is designed for loading and managing financial data into a database.

Key Processes:
1. FileManager Initialisation:
   - Initialises FileManager for file operations, particularly for loading data.

2. Data Loading:
   - Loads clean financial data from a specified directory using FileManager.

3. DatabaseHandler Initialisation:
   - Establishes a connection to the database using DatabaseHandler with specified credentials.

4. Table Creation in Database:
   - Defines the schema for the financial data table, including field names and constraints.
   - Creates a new table in the database with the defined schema.

5. Data Insertion:
   - Inserts the loaded financial data into the newly created table in the database.

6. Connection Closure:
   - Closes the database connection upon completion of the data loading process.

Usage:
- Executes as the main module, specifically handling the loading of 'Spend' data into a database.
- Facilitates the integration of financial data into a structured database environment for further analysis or reporting.

Note:
- The script assumes that the database and table do not already exist and handles their creation.
- It's part of a larger data pipeline focusing on the integration of financial data into a database.
"""

# Custom imports
from constants import FileDirectory, Spend
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from utility.file_manager import FileManager
from utility.logging import setup_logging
from utility.database_handler import DatabaseHandler

# Initialise logging
logger = setup_logging()

##################################################################################################################################

# Main function to coordinate the script
def spend_loader():
    
    ## 1: Load Data
    # Initialise FileManager Class
    file_manager = FileManager()
    # Load clean data
    df = file_manager.load_file(FileDirectory.CLEAN_DATA_PATH, Spend.CLEAN_DATA)
    
    ## 2: Database Connection
    # Initialise the DatabaseHandler with your database credentials
    db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
    
    ## 3: Table Creation 
    # Define fields for your table
    fields_constraints = {
        'transaction_id': 'INT UNSIGNED NOT NULL PRIMARY KEY',
        'category_a': 'VARCHAR(50) NOT NULL',
        'category_b': 'VARCHAR(50) NOT NULL',
        'outlet': 'VARCHAR(50) NOT NULL',
        'description': 'TEXT',
        'amount': 'DECIMAL(7,2)',
        'date': 'DATE NOT NULL',
        'period': 'VARCHAR(8)',
    }

    # Create the table
    db_handler.create_table(Spend.CLEAN_DATA.split('.')[0], fields_constraints)

    ## 4: Data Insertion 
    # Extracting the keys from the fields dictionary
    # Insert data into the table
    db_handler.insert_data(Spend.CLEAN_DATA.split('.')[0], df, list(fields_constraints.keys()))
    
    ## 5: Close connection
    # Close the database connection when done
    db_handler.close_connection()

if __name__ == '__main__':
    spend_loader()

