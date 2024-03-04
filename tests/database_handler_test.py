# Import the required libraries
import sys  # For Python interpreter control

# Configuration
sys.dont_write_bytecode = True  # Prevent Python from writing bytecode files (.pyc)
sys.path.append('/Users/hadid/Projects/ETL')  # Add path to system path

# Custom imports
from constants import FileDirectory, Spend
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from utility.file_manager import FileManager
from utility.logging import setup_logging
from utility.database_handler import DatabaseHandler

# Setting up logging
setup_logging()

##################################################################################################################################

# Main function to coordinate the script
def spend_loader():
    
    ## 1: Load Data
    # Initialize FileManager Class
    file_manager = FileManager()
    # Load clean data
    df = file_manager.load_file(FileDirectory.CLEAN_DATA_PATH, Spend.CLEAN_DATA)
    
    ## 2: Database Connection
    # Initialize the DatabaseHandler with your database credentials
    db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
    
    ## 3: Table Creation 
    # Define fields for your table
    fields = {
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
    db_handler.create_table('spend', fields)

    ## 4: Data Insertion 
    # Define the column mapping for the insert operation
    column_mapping = ['transaction_id', 'category_a', 'category_b', 'outlet', 'description', 'amount', 'date', 'period']

    # Insert data into the table
    db_handler.insert_data('spend', df, column_mapping)
    
    ## 5: Close connection
    # Close the database connection when done
    #db_handler.close_connection()



def test_spend_loader():
    # Testing table creation and data insertion
    #print("Testing table creation and data insertion...")
    spend_loader()
    #print("Initial data load completed.")

    # Re-testing to see behavior when table and data already exist
    #print("Re-testing data load to check behavior for existing table and data...")
    spend_loader()
    #print("Re-test data load completed.")

def test_drop_table(db_handler):
    # Testing dropping of the table
    #print("Testing dropping of the 'spend' table...")
    db_handler.drop_table('spend')
    #print("'spend' table dropped.")

    # Re-testing drop to see behavior when table does not exist
    #print("Re-testing drop table to check behavior for non-existing table...")
    db_handler.drop_table('spend')
    #print("Re-test drop table completed.")

if __name__ == '__main__':
    
    db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

    test_spend_loader()  # Test the spend_loader function

    # Uncomment the line below to test dropping the 'spend' table
    test_drop_table(db_handler)

    db_handler.close_connection()  # Close the database connection
