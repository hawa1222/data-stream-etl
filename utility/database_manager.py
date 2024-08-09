"""
Script to handle database operations such as connecting to a MySQL database,
creating and dropping tables, and inserting data.
"""

import sys

import mysql.connector
import pandas as pd

from mysql.connector import Error

from utility.log_manager import setup_logging

logger = setup_logging()


class DatabaseHandler:
    """
    Class for handling database operations such as connecting to a MySQL database,
    creating and dropping tables, and inserting data. It's designed to be flexible
    for various database operations.

    Attributes:
        host (str): Database server host.
        port (int): Port number to use for connection.
        user (str): Username used to authenticate with database.
        password (str): Password used to authenticate with database.
        db_name (str): Name of database to use.
        connection (mysql.connector.connection.MySQLConnection): Connection object to database.
    """

    def __init__(self, host, port, user, password, db_name):
        """
        Initialises DatabaseHandler instance with provided database credentials.

        Parameters:
            host (str): The database server host.
            port (int): The port number for database server.
            user (str): The username for database.
            password (str): The password for database.
            db_name (str): The name of database.
        """
        self.host = host  # Database server host
        self.port = port  # Port number for database server
        self.user = user  # Username for database access
        self.password = password  # Password for database access
        self.db_name = db_name  # Name of database

        self.connection = self.connect()  # Establish database connection

    def connect(self):
        """
        Connects to MySQL database using stored credentials and sets time zone to UTC.

        Returns:
            Connection object if successful; otherwise, script exits.

        Raises:
            SystemExit: If connection to database fails.
        """
        try:
            # Attempt to create a database connection
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.db_name,
            )

            # Create a cursor and set time zone to UTC
            cursor = conn.cursor()
            cursor.execute("SET time_zone = '+00:00';")
            cursor.execute("SET SESSION sql_mode = '';")
            cursor.close()

            logger.info("Successfully connected to database and set time zone to UTC")
            return conn
        except Error as e:
            # Log error and exit script if connection fails
            logger.error("Error while connecting to MySQL: %s", e)
            sys.exit(1)

    def create_table(self, table_name, fields):
        """
        Creates a table in database if it doesn't already exist.
        This method expects all field definitions, including primary keys and foreign keys,
        to be included directly within field definitions in 'fields' dictionary.

        Parameters:
            table_name (str): Name of table to create.
            fields (dict): Dictionary of field names with their SQL data types and constraints,
            including primary and foreign keys.
        """
        if self.connection.is_connected():
            with (
                self.connection.cursor() as cursor
            ):  # Managing cursor using 'with' statement for automatic cleanup
                try:
                    # Execute a SQL query to check if table already exists
                    cursor.execute(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        f"WHERE table_schema = '{self.connection.database}' "
                        f"AND table_name = '{table_name}'"
                    )
                    if cursor.fetchone()[0] == 1:  # Check if table exists
                        logger.info(f"Table '{table_name}' already exists")  # Log if table exists
                    else:
                        # Generate SQL query for creating table
                        create_table_query = self.generate_create_table_query(table_name, fields)
                        # logger.info(f"Generated SQL for table creation: {create_table_query}")
                        cursor.execute(create_table_query)  # Execute SQL command to create table
                        logger.info(
                            f"Table '{table_name}' created successfully"
                        )  # Log successful table creation

                        # Log details of any foreign keys defined within column definitions
                        for field, definition in fields.items():
                            if "FOREIGN KEY" in definition:
                                # Extract and log foreign key details from column definition
                                fk_detail = definition.split("FOREIGN KEY")[1].strip()
                                logger.debug(
                                    f"Foreign key defined '{field}' for table "
                                    f"'{table_name}': {fk_detail}"
                                )

                except mysql.connector.ProgrammingError as pe:
                    logger.error(
                        f"Programming Error during table creation: {pe}"
                    )  # Log programming errors
                except mysql.connector.IntegrityError as ie:
                    logger.error(
                        f"Integrity Error during table creation: {ie}"
                    )  # Log integrity errors
                except Error as e:
                    logger.error(
                        f"General Error during table creation: {e}"
                    )  # Log any other errors

    def generate_create_table_query(self, table_name, fields):
        """
        Generates SQL query to create table, including fields and any inline primary
        or foreign key definitions.

        Parameters:
            table_name (str): Name of table.
            fields (dict): Field names with their SQL data types and constraints,
            including primary and foreign keys.

        Returns:
            str: A SQL query string for creating specified table.
        """
        # Combine field definitions into a string
        field_definitions = ", ".join(
            [f"{field} {definition}" for field, definition in fields.items()]
        )

        # Construct complete CREATE TABLE SQL query
        create_table_query = (
            f"CREATE TABLE IF NOT EXISTS {table_name} ({field_definitions}) ENGINE=InnoDB;"
        )

        # Return complete SQL query
        return create_table_query

    def drop_table(self, table_name):
        """
        Drops (deletes) a table from database if it exists.

        Parameters:
            table_name (str): Name of table to be dropped.
        """
        if self.connection.is_connected():
            with self.connection.cursor() as cursor:
                try:
                    # First, check if table exists
                    cursor.execute(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        f"WHERE table_schema = '{self.connection.database}' "
                        f"AND table_name = '{table_name}'"
                    )
                    if cursor.fetchone()[0] == 0:
                        logger.info(f"Table '{table_name}' does not exist, no action taken.")
                    else:
                        # Table exists, proceed to drop it
                        drop_table_query = f"DROP TABLE {table_name};"
                        cursor.execute(drop_table_query)
                        logger.info(f"Table '{table_name}' dropped successfully")
                except Error as e:
                    logger.error(f"Error while dropping table '{table_name}': {e}")

    def insert_data(self, table_name, data_frame, column_mapping):
        """
        Inserts data into specified table from a pandas DataFrame.

        Parameters:
            table_name (str): Name of table where data will be inserted.
            data_frame (pandas.DataFrame): DataFrame containing data to insert.
            column_mapping (list): List of DataFrame column names corresponding to table fields.
        """
        # Check if connection to database is active
        if self.connection.is_connected():
            try:
                # Use 'with' statement for resource management of cursor
                with self.connection.cursor() as cursor:
                    # Check if table exists before attempting to insert data
                    cursor.execute(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        f"WHERE table_schema = '{self.connection.database}' "
                        f"AND table_name = '{table_name}'"
                    )
                    if cursor.fetchone()[0] == 0:
                        logger.error(
                            f"Failed to insert data: Table '{table_name}' does not exist."
                        )
                        return  # Exit function as table doesn't exist
                    # Initialise counters for added and updated records
                    added_count = 0
                    updated_count = 0

                    # Generate SQL query for data insertion
                    insert_query = self.generate_insert_query(table_name, column_mapping)

                    # Iterate over each row in  DataFrame
                    for i, row in data_frame.iterrows():
                        # Prepare a tuple of data for insertion, handling null values
                        data_tuple = tuple(
                            row[column] if pd.notna(row[column]) else None
                            for column in column_mapping
                        )
                        try:
                            # Execute insert query with prepared data tuple
                            cursor.execute(insert_query, data_tuple)

                            # Check no. of affected rows to determine if row was added or updated
                            if cursor.rowcount == 1:
                                added_count += 1  # Increment added count for a new row
                            elif cursor.rowcount == 2:
                                updated_count += 1  # Increment updated count for an existing row

                            # Commit transaction to save changes
                            self.connection.commit()
                        except Error as e:
                            # Log any error that occurs during insert operation
                            logger.error("Error on insert at index %d: %s", i, e)
                            # Rollback transaction to revert changes since last commit
                            self.connection.rollback()

                    # Log number of records added and updated after completing insertion process
                    logger.info(
                        f"Data insertion process completed for table '{table_name}': "
                        f"%d records added, %d records updated",
                        added_count,
                        updated_count,
                    )

            except Error as e:
                # Log any error that occurs during database operation outside row insertion
                logger.error("Error during data insertion into table '%s': %s", table_name, e)
                # Rollback any changes made in current transaction
                self.connection.rollback()

    def generate_insert_query(self, table_name, column_mapping):
        """
        Generates a SQL query for inserting data into a table.

        Parameters:
            table_name (str): Name of table.
            column_mapping (list): List of column names for INSERT query.

        Returns:
            str: A SQL query string for inserting data.
        """
        # Construct column names and placeholders for INSERT query
        columns = ", ".join(column_mapping)
        values_placeholders = ", ".join(["%s"] * len(column_mapping))

        # Construct ON DUPLICATE KEY UPDATE clause
        update_clause = ", ".join([f"{col} = VALUES({col})" for col in column_mapping])

        # Return complete INSERT INTO SQL query
        return f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({values_placeholders})
        ON DUPLICATE KEY UPDATE
        {update_clause}
        """

    def fetch_data(self, tables):
        """
        Fetches data from database for given table(s) and returns it as dict of Pandas DataFrames.

        Parameters:
            tables: A single table name or a list of table names to fetch.

        Returns:
            dict: A dictionary of DataFrames with each table's data.
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
                logger.info(f"Data fetched successfully for table: {table}")
            except Error as e:
                logger.error(f"Error fetching data from table {table}: {e}")
                dataframes[table] = pd.DataFrame()  # Return an empty DataFrame on error

        return dataframes

    def close_connection(self):
        """
        Closes database connection if it is open.

        This method provides a safe and reusable way to close database connection,
        ensuring that connection is properly closed and resources are released.
        """
        if self.connection.is_connected():
            try:
                self.connection.close()  # Attempt to close connection
                logger.info("Database connection closed successfully.")
            except Error as e:
                # Log any error that occurs while trying to close connection
                logger.error("Error while closing database connection: %s", e)
        else:
            # If connection is already closed, log this information
            logger.info("Database connection is already closed.")
