import os
import pandas as pd
from collections import defaultdict
import xml.etree.ElementTree as ET

# =============================================================================
# import sys
# # Prevent bytecode (.pyc) file generation
# sys.dont_write_bytecode = True
# sys.path.append('/Users/hadid/GitHub/ETL')  # Add path to system path
# =============================================================================

from constants import FileDirectory, AppleHealth
from utility.standardise_fields import DataStandardiser
from utility.file_manager import FileManager

# Import and set up logging
from utility.logging import setup_logging
logger = setup_logging()

##################################################################################################################################

## Load all Elements

# Initialise FileManager Class
file_manager = FileManager()

# Load the Apple Health export.xml file from iCloud
tree = file_manager.load_file(FileDirectory.MANUAL_EXPORT_PATH, 'apple_health_export/export.xml')
root = tree.getroot()

## Examine XML Structure

unique_elements = {elem.tag for elem in tree.iter()}
print("Unique elements in the XML:", unique_elements)

# Initialize an empty dictionary to store unique element-attribute pairs
unique_structure = {}

# Recursive function to collect unique elements and attributes
def collect_unique_structure(element, path=''):
    # Build the path for the current element
    current_path = f"{path}/{element.tag}"

    # Update the dictionary with unique attributes of the current element
    if current_path not in unique_structure:
        unique_structure[current_path] = set()
    unique_structure[current_path].update(element.attrib.keys())

    # Recursively apply the function to all child elements
    for child in element:
        collect_unique_structure(child, current_path)

# Start the traversal from the root element
collect_unique_structure(tree.getroot())

# Print out the unique structure
for path, attrs in unique_structure.items():
    print(f"Path: {path}, Unique Attribute Names: {list(attrs)}")




##################################################################################################################################

## Create dictionary with data from each element (max 100k rows)

# Initialise a defaultdict to hold data for each unique element
element_data = defaultdict(list)

# Loop through the XML tree to populate element_data
for elem in tree.iter():
    # If we have fewer than 100k rows for this element type, add to list
    if len(element_data[elem.tag]) < 100000:
        element_data[elem.tag].append(elem.attrib)

# Dictionary to hold DataFrames
data_frames = {}

# Create a DataFrame for each unique element and store in data_frames
for elem_tag, attrib_list in element_data.items():
    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(attrib_list)

    # Add this DataFrame to data_frames dictionary
    data_frames[elem_tag] = df

    print(f"DataFrame for {elem_tag} has been created with {len(df)} rows.")


##################################################################################################################################

# Clean WorkoutStatistics
df = data_frames['WorkoutStatistics'].copy()

# Remove 'HKQuantityType' from the 'type' column
df['type'] = df['type'].str.replace('HKQuantityTypeIdentifier', '')

# Get all unique 'type'
unique_types = df['type'].unique()

# Create an empty DataFrame to store the final data
final_df = pd.DataFrame()

# Loop through all unique types
for unique_type in unique_types:
    # Filter rows based on the unique type
    sub_df = df[df['type'] == unique_type]

    # Use 'startDate' and 'endDate' as a primary key
    sub_df['primary_key'] = sub_df['startDate'] + "_" + sub_df['endDate']

    # Create new columns with the format 'type_field_unit'
    for field in ['sum', 'average', 'minimum', 'maximum']:
        new_col_name = f"{unique_type}_{field}_{sub_df.iloc[0]['unit']}"  # The new column name
        sub_df[new_col_name] = sub_df[field]  # Populate the new column with data

    # Drop original columns to avoid redundancy
    sub_df.drop(['type', 'startDate', 'endDate', 'sum', 'unit', 'average', 'minimum', 'maximum'], axis=1, inplace=True)

    # If final_df is empty, initialize it with sub_df
    if final_df.empty:
        final_df = sub_df
    else:
        # Merge with the existing DataFrame based on 'primary_key'
        final_df = pd.merge(final_df, sub_df, on='primary_key', how='outer')

final_df.dropna(axis=1, how='all', inplace=True)

# Create an instance of the DataStandardiser class
standardiser = DataStandardiser()
new_df = standardiser.standardise_df(final_df)


##################################################################################################################################

