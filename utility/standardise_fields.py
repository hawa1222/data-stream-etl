# Import required libraries
import re  # Importing the regular expressions library for string manipulation
import json  # Importing the JSON library for JSON data handling
import pandas as pd  # Importing the pandas library for DataFrame operations

# Custom imports
from utility.logging import setup_logging  # Custom logging setup

# Call the logging setup function to initialise logging
logger = setup_logging()

#############################################################################################

class DataStandardiser:
    def __init__(self):
        """
        Initialise the DataStandardiser class.
        
        Attributes:
        col_counts (dict): Dictionary to keep track of column name frequencies.
        reserved_words (set): Set of Python reserved words.
        """
        # Initialise an empty dictionary to track the frequency of each column name
        self.col_counts = {}
        
        # Set of Python reserved words to avoid conflicts
        self.reserved_words = {'def', 'return', 'if', 'else', 'while', 'for', 'break'}
    
    def remove_all_nan_rows(self, df):
        """
        Remove rows where all fields have NaN values.
    
        Parameters:
        df (DataFrame): The original DataFrame.
    
        Returns:
        DataFrame: The DataFrame without rows where all fields are NaN.
        """
        # Remove rows where all fields are NaN
        new_df = df.dropna(how='all').reset_index(drop=True)
    
        logger.info('Removed rows with NaN in all fields.')
    
        return new_df
    
    def standardise_col(self, col):
        """
        Standardise a single column name according to specified rules.
        
        Parameters:
        col (str): The original column name.
        
        Returns:
        str: The standardised column name.
        """
        # If the column name is empty, default it to 'unnamed'
        if col.strip() == '':
            col = 'unnamed'
        
        # First, remove all leading and trailing spaces from the column name
        col = col.strip()
        
        # Then, replace all spaces (including consecutive spaces) with a single underscore
        col = re.sub(r'\s+', '_', col)


        # Add underscores before each capital letter (but not at the beginning of the string)
        col = re.sub('(?<=[a-z])(?=[A-Z])', '_', col)
        
        # Convert all characters to lowercase
        col = col.lower()
        
        # Remove any special characters, retaining only alphanumeric characters and underscores
        col = re.sub('[^a-zA-Z0-9_]', '', col).strip()
        
        # If the column starts with a number, prepend it with 'field_'
        if col[0].isdigit():
            col = 'field_' + col
        
        # If the column is a Python reserved word, append '_col' to it
        if col in self.reserved_words:
            col = col + '_col'
        
        # Handle duplicate column names by appending a number to them
        if col in self.col_counts:
            self.col_counts[col] += 1
            col = f"{col}_{self.col_counts[col]}"
        else:
            self.col_counts[col] = 1
                
        # Return the standardised column name
        return col
    
    def standardise_df(self, df, remove_nans=None):
        """
        Standardise all column names in a DataFrame and optionally remove rows where all fields are NaN.
        
        Parameters:
        df (DataFrame): The original DataFrame.
        remove_nans (DataFrame, optional): If set to 'Y', all rows where all fields are NaN are removed.
        
        Returns:
        DataFrame: The DataFrame with standardised column names and optionally without rows where all fields are NaN.
        """
        
        self.col_counts = {}  # Reset the column counts
        
        if remove_nans == 'Y':
            df = self.remove_all_nan_rows(df)
        
        # Standardise each column name and store them in a list
        new_columns = [self.standardise_col(col) for col in df.columns]
        
        # Update the DataFrame column names with the standardised ones
        df.columns = new_columns
        logger.info('Field names for standardised')
        
        return df
    
    def json_normalise(self, api_response, one_level_above=False):
        """
        Normalise and standardise columns of a DataFrame generated from a JSON-like API response.
        
        Parameters:
        api_response (str, list, dict): The original API response.
        one_level_above (bool): Flag to control column name granularity.
        
        Returns:
        DataFrame: The DataFrame with normalised and standardised column names.
        """
        # Check the type of API response. If it's a string, convert it to JSON; if it's a list or dictionary, use as is.
        if isinstance(api_response, str):
            json_data = json.loads(api_response)
        elif isinstance(api_response, (list, dict)):
            json_data = api_response
        else:
            raise ValueError("Invalid type for api_response.")
        
        # Flatten the JSON data into a DataFrame
        df = pd.json_normalize(json_data)
        
        # Dictionary to hold new column names
        new_columns = {}
        
        # Iterate through each column name in the DataFrame
        for col in df.columns:
            # Split the column name into its constituent levels based on the '.' delimiter
            levels = col.split('.')
            
            # Conditionally set new column names based on the 'one_level_above' flag and number of levels
            if one_level_above and len(levels) > 2:
                new_col_name = f"{levels[-2]}_{levels[-1]}"
            else:
                new_col_name = levels[-1]
            
            # Populate the new column names into the dictionary
            new_columns[col] = new_col_name
        
        # Rename the DataFrame's columns
        df.rename(columns=new_columns, inplace=True)
        
        logger.info('JSON data normalised')
        
        # Return the DataFrame with standardised column names
        return self.standardise_df(df)
