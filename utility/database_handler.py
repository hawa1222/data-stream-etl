# Import the required libraries
import mysql.connector
from mysql.connector import Error
import logging
import pandas as pd


# =============================================================================
import sys
# # Add the path to the directory containing utils.py to sys.path
# sys.dont_write_bytecode = True
# sys.path.append('/Users/hadid/GitHub/ETL')  # Add path to system path
# =============================================================================

# Custom imports
from utility.logging import setup_logging

# Initialise logging
setup_logging()

##################################################################################################################################

class DatabaseHandler:
    """
    Class for handling database operations such as connecting to a MySQL database, 
    creating and dropping tables, and inserting data. It's designed to be flexible 
    for various database operations.
    
    Attributes:
    host (str): The database server host.
    port (int): The port number to use for the connection.
    user (str): The username used to authenticate with the database.
    password (str): The password used to authenticate with the database.
    db_name (str): The name of the database to use.
    connection (mysql.connector.connection_cext.CMySQLConnection): The connection object to the database.
    """

    def __init__(self, host, port, user, password, db_name):
        """
        Initialises the DatabaseHandler instance with the provided database credentials.
        
        Parameters:
        host (str): The database server host.
        port (int): The port number for the database server.
        user (str): The username for the database.
        password (str): The password for the database.
        db_name (str): The name of the database.
        """
        self.host = host  # Database server host
        self.port = port  # Port number for the database server
        self.user = user  # Username for database access
        self.password = password  # Password for database access
        self.db_name = db_name  # Name of the database
        
        self.connection = self.connect()  # Establish database connection

    def connect(self):
        """
        Connects to the MySQL database using the stored credentials and sets the time zone to UTC.
    
        Returns:
        Connection object if successful; otherwise, the script exits.
    
        Raises:
        SystemExit: If connection to the database fails.
        """
        try:
            # Attempt to create a database connection
            conn = mysql.connector.connect(
                host=self.host, port=self.port, user=self.user, password=self.password, database=self.db_name)
    
            # Create a cursor and set the time zone to UTC
            cursor = conn.cursor()
            cursor.execute("SET time_zone = '+00:00';")
            cursor.close()
    
            logging.info("Successfully connected to the database and set time zone to UTC")
            return conn
        except Error as e:
            # Log the error and exit the script if connection fails
            logging.error("Error while connecting to MySQL: %s", e)
            sys.exit(1)

    def create_table(self, table_name, fields):
        """
        Creates a table in the database if it doesn't already exist. This method expects all field definitions,
        including primary keys and foreign keys, to be included directly within the field definitions in the 'fields' dictionary.
    
        Parameters:
        table_name (str): Name of the table to create.
        fields (dict): Dictionary of field names with their SQL data types and constraints, including primary and foreign keys.
        """
        if self.connection.is_connected():
            with self.connection.cursor() as cursor:  # Managing the cursor using 'with' statement for automatic cleanup
                try:
                    # Execute a SQL query to check if the table already exists
                    cursor.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{self.connection.database}' AND table_name = '{table_name}'")
                    if cursor.fetchone()[0] == 1:  # Check if the table exists
                        logging.info(f"Table '{table_name}' already exists")  # Log if table exists
                    else:
                        # Generate the SQL query for creating the table
                        create_table_query = self.generate_create_table_query(table_name, fields)
                        #logging.info(f"Generated SQL for table creation: {create_table_query}")  # Log the SQL command for debugging
                        cursor.execute(create_table_query)  # Execute the SQL command to create the table
                        logging.info(f"Table '{table_name}' created successfully")  # Log successful table creation
    
                        # Log the details of any foreign keys defined within the column definitions
                        for field, definition in fields.items():
                            if 'FOREIGN KEY' in definition:
                                # Extract and log the foreign key details from the column definition
                                fk_detail = definition.split('FOREIGN KEY')[1].strip()
                                logging.info(f"Foreign key defined '{field}' for table '{table_name}': {fk_detail}")
    
                except mysql.connector.ProgrammingError as pe:
                    logging.error(f"Programming Error during table creation: {pe}")  # Log programming errors
                except mysql.connector.IntegrityError as ie:
                    logging.error(f"Integrity Error during table creation: {ie}")  # Log integrity errors
                except Error as e:
                    logging.error(f"General Error during table creation: {e}")  # Log any other errors
    
    def generate_create_table_query(self, table_name, fields):
        """
        Generates a SQL query to create a table, including fields and any inline primary or foreign key definitions.
    
        Parameters:
        table_name (str): Name of the table.
        fields (dict): Field names with their SQL data types and constraints, including primary and foreign keys.
    
        Returns:
        str: A SQL query string for creating the specified table.
        """
        # Combine field definitions into a string
        field_definitions = ', '.join([f"{field} {definition}" for field, definition in fields.items()])
    
        # Construct the complete CREATE TABLE SQL query
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({field_definitions}) ENGINE=InnoDB;"
    
        # Return the complete SQL query
        return create_table_query

    def drop_table(self, table_name):
        """
        Drops (deletes) a table from the database if it exists.
    
        Parameters:
        table_name (str): Name of the table to be dropped.
        """
        if self.connection.is_connected():
            with self.connection.cursor() as cursor:
                try:
                    # First, check if the table exists
                    cursor.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{self.connection.database}' AND table_name = '{table_name}'")
                    if cursor.fetchone()[0] == 0:
                        logging.info(f"Table '{table_name}' does not exist, no action taken.")
                    else:
                        # Table exists, proceed to drop it
                        drop_table_query = f"DROP TABLE {table_name};"
                        cursor.execute(drop_table_query)
                        logging.info(f"Table '{table_name}' dropped successfully")
                except Error as e:
                    logging.error(f"Error while dropping table '{table_name}': {e}")

    def insert_data(self, table_name, data_frame, column_mapping):
        """
        Inserts data into the specified table from a pandas DataFrame.
    
        Parameters:
        table_name (str): Name of the table where data will be inserted.
        data_frame (pandas.DataFrame): DataFrame containing the data to insert.
        column_mapping (list): List of DataFrame column names corresponding to table fields.
        """
        # Check if the connection to the database is active
        if self.connection.is_connected():
            try:
                # Use 'with' statement for resource management of the cursor
                with self.connection.cursor() as cursor:
                    # Check if the table exists before attempting to insert data
                    cursor.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{self.connection.database}' AND table_name = '{table_name}'")
                    if cursor.fetchone()[0] == 0:
                        logging.error(f"Failed to insert data: Table '{table_name}' does not exist.")
                        return  # Exit the function as the table doesn't exist
                    # Initialise counters for added and updated records
                    added_count = 0
                    updated_count = 0
    
                    # Generate the SQL query for data insertion
                    insert_query = self.generate_insert_query(table_name, column_mapping)
    
                    # Iterate over each row in the DataFrame
                    for i, row in data_frame.iterrows():
                        # Prepare a tuple of data for insertion, handling null values
                        data_tuple = tuple(row[column] if pd.notna(row[column]) else None for column in column_mapping)
                        try:
                            # Execute the insert query with the prepared data tuple
                            cursor.execute(insert_query, data_tuple)
    
                            # Check the number of affected rows to determine if the row was added or updated
                            if cursor.rowcount == 1:
                                added_count += 1  # Increment added count for a new row
                            elif cursor.rowcount == 2:
                                updated_count += 1  # Increment updated count for an existing row
    
                            # Commit the transaction to save changes
                            self.connection.commit()
                        except Error as e:
                            # Log any error that occurs during the insert operation
                            logging.error("Error on insert at index %d: %s", i, e)
                            # Rollback the transaction to revert changes since the last commit
                            self.connection.rollback()
    
                    # Log the number of records added and updated after completing the insertion process
                    logging.info(f"Data insertion process completed for table '{table_name}': %d records added, %d records updated", added_count, updated_count)
                    
            except Error as e:
                # Log any error that occurs during the database operation outside the row insertion
                logging.error("Error during data insertion into table '%s': %s", table_name, e)
                # Rollback any changes made in the current transaction
                self.connection.rollback()

    def generate_insert_query(self, table_name, column_mapping):
    
        """
        Generates a SQL query for inserting data into a table.

        Parameters:
        table_name (str): Name of the table.
        column_mapping (list): List of column names for the INSERT query.

        Returns:
        str: A SQL query string for inserting data.
        """
        # Construct the column names and placeholders for the INSERT query
        columns = ', '.join(column_mapping)
        values_placeholders = ', '.join(['%s'] * len(column_mapping))
        
        # Construct the ON DUPLICATE KEY UPDATE clause
        update_clause = ', '.join([f"{col} = VALUES({col})" for col in column_mapping])
        
        # Return the complete INSERT INTO SQL query
        return f'''
        INSERT INTO {table_name} ({columns})
        VALUES ({values_placeholders})
        ON DUPLICATE KEY UPDATE
        {update_clause}
        '''
    
    def fetch_data(self, tables):
        """
        Fetches data from the database for the given table(s) and returns it as a dictionary of Pandas DataFrames.

        :param tables: A single table name or a list of table names to fetch.
        :return: Dictionary of DataFrames with each table's data.
        """
        dataframes = {}
        
        # Ensure tables is a list even if a single table name is provided
        if isinstance(tables, str):
            tables = [tables]

        for table in tables:
            query = f"SELECT * FROM {table};"
            try:
                with self.connection.cursor(dictionary=True) as cursor:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    dataframes[table] = pd.DataFrame(result)
                logging.info(f"Data fetched successfully for table: {table}")
            except Error as e:
                logging.error(f"Error fetching data from table {table}: {e}")
                dataframes[table] = pd.DataFrame()  # Return an empty DataFrame on error

        return dataframes
        
    def close_connection(self):
        """
        Closes the database connection if it is open.

        This method provides a safe and reusable way to close the database connection,
        ensuring that the connection is properly closed and resources are released.
        """
        if self.connection.is_connected():
            try:
                self.connection.close()  # Attempt to close the connection
                logging.info("Database connection closed successfully.")
            except Error as e:
                # Log any error that occurs while trying to close the connection
                logging.error("Error while closing database connection: %s", e)
        else:
            # If the connection is already closed, log this information
            logging.info("Database connection is already closed.")






