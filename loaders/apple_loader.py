"""
This script is designed to load Apple Health data into a database after creating the necessary tables.

Key Processes:
1. FileManager and DatabaseHandler Initialisation:
   - Initialises FileManager for handling file operations and DatabaseHandler for database interactions.

2. Field Structure Definition:
   - Defines the field structures (schema) for various Apple Health datasets like walking metrics, 
   daily activity, steps, running metrics, and sleep data.

3. Data Loading:
   - Loads each Apple Health dataset from Excel files into pandas DataFrames.

4. Table Creation and Data Insertion:
   - Iterates through each dataset:
     - Creates a table in the database with the defined schema for that dataset.
     - Inserts the corresponding data from the DataFrame into the newly created table.

5. Connection Closure:
   - Closes the database connection after all operations are completed.

Usage:
- Executes as the main module to handle the loading of structured Apple Health data into a relational database.
- Ensures that data from various health metrics is organised and stored efficiently for further analysis or reporting.

Note:
- The script assumes that the data files are in the Excel format and that the database does not contain the tables beforehand.
- It's a part of a larger data processing system focusing on health data management and analysis.
"""


# Import the required libraries
import sys

# Configuration
sys.dont_write_bytecode = True  # Prevent Python from writing bytecode files (.pyc)
sys.path.append('/Users/hadid/Projects/ETL')  # Add path to system path

# Custom imports
from constants import FileDirectory
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from utility.file_manager import FileManager
from utility.logging import setup_logging
from utility.database_handler import DatabaseHandler

# Initialise logging
setup_logging()

##################################################################################################################################

def apple_loader():
    # Initialise FileManager Class
    file_manager = FileManager()

    # Dictionary mapping dataset names to their corresponding field structures.
    field_structures = {
        'apple_walking_metrics': {
            'date': 'DATE NOT NULL PRIMARY KEY',
            'walking_steadines_pct': 'DECIMAL(5,2)',
            'walking_asymm_pct': 'DECIMAL(5,2)',
            'walking_ds_pct': 'DECIMAL(5,2)',
            'walking_avg_HR': 'DECIMAL(5,2)',
            'walking_speed_kmhr': 'DECIMAL(5,2)',
            'walking_step_len_cm': 'DECIMAL(5,2)'
        },
        'apple_daily_activity': {
            'date': 'DATE NOT NULL PRIMARY KEY',
            'basal_energy_kcal': 'DECIMAL(6,2)',
            'flight_climbed': 'SMALLINT UNSIGNED',
            'mindful_duration': 'DECIMAL(5,2)',
            'mindful_count': 'TINYINT UNSIGNED',
            'active_energy_burned': 'DECIMAL(6,2)',
            'apple_exercise_time': 'SMALLINT UNSIGNED',
            'apple_stand_hours': 'TINYINT UNSIGNED'
        },
        'apple_steps': {
            'date': 'DATE NOT NULL',
            'hour': 'TINYINT UNSIGNED NOT NULL',
            'step_count': 'SMALLINT UNSIGNED',
            'PRIMARY KEY': '(date, hour)'
        },
        'apple_running_metrics': {
            'date': 'DATE NOT NULL PRIMARY KEY',
            'avg_run_gct_ms': 'DECIMAL(5,2)',
            'avg_run_pwr_w': 'DECIMAL(5,2)',
            'avg_run_spd_kmh': 'DECIMAL(4,2)',
            'run_stride_len_m': 'DECIMAL(4,2)',
            'run_vert_osc_cm': 'DECIMAL(4,2)'
        },
        'apple_sleep': {
            'date': 'DATE NOT NULL PRIMARY KEY',
            'asleep_core': 'DECIMAL(4,2)',
            'asleep_deep': 'DECIMAL(4,2)',
            'asleep_rem': 'DECIMAL(4,2)',
            'asleep_unspecified': 'DECIMAL(4,2)',
            'awake': 'DECIMAL(4,2)',
            'total_sleep': 'DECIMAL(4,2)',
            'bed_time': 'TIMESTAMP',
            'awake_time': 'TIMESTAMP',
            'source_name': 'VARCHAR(50)',
            'time_in_bed': 'DECIMAL(4,2)'
        },
        'apple_fitness_metrics': {
            'date': 'DATE NOT NULL PRIMARY KEY',
            'avg_1min_HR_recovery': 'DECIMAL(5,2)',
            'avg_HR_variability': 'DECIMAL(5,2)',
            'avg_oxg_saturation': 'DECIMAL(5,2)',
            'avg_respiratory_pm': 'DECIMAL(5,2)',
            'avg_resting_HR': 'DECIMAL(5,2)',
            'vo2Max_mLmin_kg': 'DECIMAL(5,2)'
        },''
        'apple_low_hr_events': {
            'date': 'DATE NOT NULL',
            'hour': 'TINYINT UNSIGNED NOT NULL',
            'low_HR_event': 'TINYINT UNSIGNED',
            'PRIMARY KEY': '(date, hour)'
        },
        'apple_blood_glucose': {
        'date': 'DATE NOT NULL',
        'hour': 'TINYINT UNSIGNED NOT NULL',
        'avg_blood_glucose_mmol': 'DECIMAL(5,2)',
        'PRIMARY KEY': '(date, hour)'
        },
        'apple_heart_rate': {
            'date': 'DATE NOT NULL',
            'hour': 'TINYINT UNSIGNED NOT NULL',
            'avg_HR_rate': 'DECIMAL(5,2)',
            'PRIMARY KEY': '(date, hour)'
        }
    }
    
    # Dictionary mapping dataset names to their file names for easier management
    datasets = {name: name + '.xlsx' for name in field_structures.keys()}

    # Load all data files and store them in a dictionary
    dataframes = {name: file_manager.load_file(FileDirectory.CLEAN_DATA_PATH, filename) for name, filename in datasets.items()}

    # Initialise the DatabaseHandler
    db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

    # Create tables and insert data for each dataset
    for name, fields in field_structures.items():
        # Create table for the dataset
        db_handler.create_table(name, fields)

        # Extract column names, excluding entries like 'PRIMARY KEY'
        column_names = [field for field in fields if not field.startswith('PRIMARY KEY')]

        # Insert data into the created table
        db_handler.insert_data(name, dataframes[name], column_names)

    # Close the database connection
    db_handler.close_connection()


if __name__ == '__main__':
    apple_loader()
    

