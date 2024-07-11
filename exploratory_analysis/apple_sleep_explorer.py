import pandas as pd

from constants import Apple, FileDirectory
from utility.clean_data import DataStandardiser
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


### Load select few elements

# Initialize FileManager Class
file_manager = FileManager()

# Load the Apple Health export.xml file from iCloud
tree = file_manager.load_file(FileDirectory.MANUAL_EXPORT_PATH, Apple.XML_DATA, "xml")

# Get the root element of the XML tree
root = tree.getroot()

# Define elements of interest
elements_of_interest = {"Record"}

# Initialize an empty dictionary to store DataFrames
dataframes_dict = {}

# Iterate over the tree only once
for elem in root.iter():
    if elem.tag in elements_of_interest:
        # Add attributes to the corresponding list in the dictionary
        dataframes_dict.setdefault(elem.tag, []).append(elem.attrib)

# Convert lists of attributes to DataFrames
for element, attributes in dataframes_dict.items():
    dataframes_dict[element] = pd.DataFrame(attributes)


### Create dfs from 'Record' element
standardiser = DataStandardiser()

records = [record.attrib for record in root.iter("Record")]  # Creates a list
records_df = pd.DataFrame(records)  # Creates a dataframe

df_standardised = standardiser.standardise_df(records_df)

# Step 1: Perform all common DataFrame manipulations
# Create a copy of the 'value' column before filling NAs
records_df["original_value"] = records_df["value"].copy()
# Convert the 'value' column to numeric, NaN if conversion fails
records_df["value"] = pd.to_numeric(records_df["value"], errors="coerce")
# Fill NaNs in 'value' column with 1.0
records_df["value"] = records_df["value"].fillna(1.0)
# Remove 'HKQuantityTypeIdentifier' from 'type' column
records_df["type"] = records_df["type"].str.split("Identifier").str.get(-1)
# Remove leading and trailing spaces from 'sourceName'
records_df["source_name"] = records_df["source_name"].str.strip()

# Step 2: Create individual DataFrames
# Get unique values from the 'type' column
unique_types = records_df["type"].unique()
unique_types = ["SleepAnalysis"]

# Iterate through each unique type and store DataFrames in a dictionary
records_dict = {}
for unique_type in records_df["type"].unique():
    # Clean and create a suitable key name
    key_name = unique_type.replace("-", "_").replace(" ", "_")
    if key_name == "" or key_name[0].isdigit():
        key_name = "df_" + key_name

    # Filter and store the DataFrame
    records_dict[key_name] = records_df[records_df["type"] == unique_type]


"""
This code block performs a series of data transformations on sleep analysis data.
It involves cleaning and extracting relevant parts from string columns,
converting dates, calculating durations, assigning priorities to different data sources,
and summarizing data in a pivoted format.
"""

# Initialize a DataFrame with sleep analysis data
df = records_dict.get("SleepAnalysis", pd.DataFrame()).copy()

# Extract the relevant part from 'original_value' column, remove everything before 'SleepAnalysis'
df["original_value"] = df["original_value"].str.split("SleepAnalysis").str.get(-1)

# Convert 'startDate', 'endDate', and 'creationDate' to datetime
df[["start_date", "end_date", "creation_date"]] = df[
    ["start_date", "end_date", "creation_date"]
].apply(pd.to_datetime)

# Calculate 'duration' in hours and extract just the date part from 'creationDate'
df["duration"] = (df["end_date"] - df["start_date"]).dt.total_seconds() / 3600
df["date"] = df["creation_date"].dt.date

# Assign priorities to each source name
priority_dict = {"HW3 iWatch": 1, "HW3 iPhone": 2, "Clock": 3, "Sleep Cycle": 4}
df["priority"] = df["source_name"].map(priority_dict)

# Sort and group by 'creationDate' to find the minimum priority for each date
df.sort_values(by=["creation_date", "priority"], inplace=True)
df = df[df["priority"] == df.groupby("date")["priority"].transform("min")]

# Group by 'creation_date' and 'original_value', then sum the 'duration' for each group
pivot_df = df.pivot_table(
    index="date", columns="original_value", values="duration", aggfunc="sum"
).reset_index()

# Add 'bed_time', 'awake_time', and 'source_name' columns to pivot_df
pivot_df["bed_time"] = (
    df.groupby("date")["start_date"].min().reset_index()["start_date"]
)
pivot_df["awake_time"] = df.groupby("date")["end_date"].max().reset_index()["end_date"]
pivot_df["time_in_bed"] = (
    pivot_df["awake_time"] - pivot_df["bed_time"]
).dt.total_seconds() / 3600
pivot_df["source_name"] = (
    df.groupby("date")["source_name"].first().reset_index()["source_name"]
)

# Step 1: Apply value from ‘AsleepUnspecified’ to ‘InBed’ if ‘InBed’ is NaN
pivot_df["InBed"] = pivot_df["InBed"].fillna(pivot_df["AsleepUnspecified"])

# Step 2: Adjust ‘Awake’ based on ‘InBed’ and ‘time_in_bed’
condition = pivot_df["Awake"].isna() & (pivot_df["InBed"] != pivot_df["time_in_bed"])
pivot_df.loc[condition, "Awake"] = pivot_df["time_in_bed"] - pivot_df["InBed"]

# Step 3: Rename ‘InBed’ to ‘total_sleep’
pivot_df.rename(columns={"InBed": "total_sleep"}, inplace=True)

pivot_df.row
