# Import the required libraries
import pandas as pd

# =============================================================================
# import sys
# # Add the path to the directory containing utils.py to sys.path
# sys.dont_write_bytecode = True
# sys.path.append('/Users/hadid/GitHub/ETL')  # Add path to system path
# =============================================================================

# Custom imports
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from utility.logging import setup_logging
from utility.database_handler import DatabaseHandler

# Setting up logging
setup_logging()

##################################################################################################################################

def create_sample_data():
    # Sample data for testing
    return pd.DataFrame({
        'transaction_id': [1, 2, 3, 4],
        'category_a': ['Groceries', 'Utilities', 'Groceries', 'Entertainment'],
        'category_b': ['Food', 'Electric', 'Food', 'Movies'],
        'outlet': ['Store A', 'Utility Company', 'Store B', 'Cinema'],
        'description': ['Apples and Bananas', 'Monthly Electric Bill', 'Bread and Butter', 'Movie Ticket'],
        'amount': [15.50, 75.00, 5.25, 12.00],
        'date': ['2023-12-01', '2023-12-02', '2023-12-03', '2023-12-04'],
        'period': ['2023-12', '2023-12', '2023-12', '2023-12']
    })

def test_table_creation(db_handler):
    #print("Testing table creation...")
    fields = {
        'transaction_id': 'INT UNSIGNED NOT NULL',
        'category_a': 'VARCHAR(50) NOT NULL',
        'category_b': 'VARCHAR(50) NOT NULL',
        'outlet': 'VARCHAR(50) NOT NULL',
        'description': 'TEXT',
        'amount': 'DECIMAL(7,2)',
        'date': 'DATE NOT NULL',
        'period': 'VARCHAR(8)',
        'PRIMARY KEY': '(transaction_id)'
    }
    db_handler.create_table('spend', fields)
    #print("Table creation tested.")

def test_data_insertion(db_handler, data):
    #print("Testing data insertion...")
    column_mapping = ['transaction_id', 'category_a', 'category_b', 'outlet', 'description', 'amount', 'date', 'period']
    db_handler.insert_data('spend', data, column_mapping)
    #print("Data insertion tested.")

def test_bulk_insertion(db_handler, data):
    #print("Testing bulk data insertion...")
    db_handler.bulk_insert_data('spend', data, column_mapping)
    #print("Bulk data insertion tested.")

def test_data_update(db_handler, data):
    #print("Testing data update...")
    # Modify a record for update testing
    data.loc[0, 'amount'] = 20.00
    db_handler.insert_data('spend', data, column_mapping)
    #print("Data update tested.")

def test_table_drop(db_handler):
    #print("Testing table dropping...")
    db_handler.drop_table('spend')
    #print("Table drop tested.")

if __name__ == '__main__':
    
    column_mapping = ['transaction_id', 'category_a', 'category_b', 'outlet', 'description', 'amount', 'date', 'period']
    
    db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
    sample_data = create_sample_data()

    test_table_creation(db_handler)  # Test table creation
    test_data_insertion(db_handler, sample_data)  # Test single data insertion
    test_bulk_insertion(db_handler, sample_data)  # Test bulk data insertion
    test_data_update(db_handler, sample_data)  # Test data update
    test_table_drop(db_handler)  # Test table dropping

    db_handler.close_connection()  # Close the database connection

##################################################################################################################################

def create_parent_table(db_handler):
    #print("Creating parent table...")
    parent_fields = {
        'category_id': 'INT UNSIGNED NOT NULL',
        'category_name': 'VARCHAR(50) NOT NULL',
        'PRIMARY KEY': '(category_id)'
    }
    db_handler.create_table('category', parent_fields)
    #print("Parent table created.")

def create_child_table_with_fk(db_handler):
    #print("Creating child table with foreign key...")
    child_fields = {
    'transaction_id': 'INT UNSIGNED NOT NULL PRIMARY KEY',
    'category_id': 'INT UNSIGNED NOT NULL, FOREIGN KEY (category_id) REFERENCES category(category_id)',
    'amount': 'DECIMAL(7,2)',
    'date': 'DATE NOT NULL'
    }

    db_handler.create_table('spend_fk', child_fields)
    #print("Child table with foreign key created.")

def insert_data_with_fk(db_handler):
    #print("Inserting data respecting foreign key constraints...")

    # Insert data into the parent table
    category_data = pd.DataFrame({
        'category_id': [1, 2],
        'category_name': ['Groceries', 'Utilities']
    })
    db_handler.bulk_insert_data('category', category_data, ['category_id', 'category_name'])

    # Insert data into the child table
    spend_data = pd.DataFrame({
        'transaction_id': [1, 2],
        'category_id': [1, 2],
        'amount': [20.00, 50.00],
        'date': ['2023-12-01', '2023-12-02']
    })
    db_handler.bulk_insert_data('spend_fk', spend_data, ['transaction_id', 'category_id', 'amount', 'date'])

    #print("Data insertion respecting foreign key constraints completed.")

def test_table_drop(db_handler):
    #print("Testing table dropping...")
    db_handler.drop_table('spend_fk')
    db_handler.drop_table('category')

    #print("Table drop tested.")
    
if __name__ == '__main__':
    db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

    create_parent_table(db_handler)  # Create parent table
    create_child_table_with_fk(db_handler)  # Create child table with foreign key
    insert_data_with_fk(db_handler)  # Insert data into both tables
    test_table_drop(db_handler)  # Test table dropping


    # Other test functions...
    
    db_handler.close_connection()  # Close the database connection

