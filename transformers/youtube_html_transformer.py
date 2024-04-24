"""
This script is responsible for transforming raw YouTube activity data from HTML format into a structured 
DataFrame and saving it as an Excel file. It performs various data manipulations to extract relevant 
information and prepare it for further analysis or reporting.

Key Functions:
1. `manipulate_activity_data(activity_df)`: Performs data manipulations on a DataFrame containing YouTube activity data. 
Extracts video IDs, standardises dates, adds additional fields, and subsets the DataFrame. Returns the manipulated DataFrame.

2. `youtube_transformer()`: Main function that serves as the entry point for transforming raw HTML 
data into a structured DataFrame and saving it as an Excel file.

Usage:
- Execute this script as the main module to transform raw YouTube activity data.
- Ensure that the raw HTML data file (containing YouTube activity data) is available in the specified directory.
- The script performs data cleaning and manipulation to prepare the data for analysis or reporting.
- The transformed data is saved as an Excel file in a structured format.

Note:
- This script is part of a larger data processing system for managing YouTube data.
- It assumes that the input data is in HTML format and contains activity information.
- The script adds fields such as video ID, thumbnail URL, channel ID, description, and source to the data.
- It standardises dates and subsets the data to keep only desired fields for further analysis.
"""

# Custom imports
from constants import FileDirectory, Youtube
from utility.file_manager import FileManager 
from utility.standardise_dates import standardise_dates  # For standardising date formats
from utility.logging import setup_logging  # Custom logging setup

# Initialise logging
logger = setup_logging()

#############################################################################################

# Define a function to manipulate a DataFrame containing activity data
def manipulate_activity_data(activity_df):
    """
    Performs various data manipulations on a DataFrame containing YouTube activity data.

    Parameters:
        activity_df (DataFrame): DataFrame containing the activity data.

    Returns:
        DataFrame: DataFrame containing the manipulated activity data.
    """

    logger.info('Cleaning data...')

    # Extract the video_id from the video_url field
    activity_df[Youtube.VID_ID] = activity_df[Youtube.VID_URL].str.split('v=').str[1].str.split('&').str[0]

    # Create a thumbnail_url field based on the video_id
    activity_df[Youtube.THUMBNAIL] = 'https://img.youtube.com/vi/' + activity_df[Youtube.VID_ID] + '/maxresdefault.jpg'

    # Extract the channel_id from the channel_url field
    activity_df[Youtube.CHANNEL_ID] = activity_df[Youtube.CHANNEL_URL].str.split('channel/').str[1].str.split('&').str[0]

    # Standardise dates
    activity_df = standardise_dates(activity_df, Youtube.DATE)

    # Create a description field and set it to None
    activity_df[Youtube.DESC] = None

    # Create a source field and set it to 'HTML'
    activity_df[Youtube.SOURCE] = Youtube.SOURCE_VALUE[1]

    # Subset set to keep only desired fields
    activity_df = activity_df[Youtube.LIKES_FIELDS]

    logger.info('Successfully cleaned HTML data')

    # Return the manipulated DataFrame
    return activity_df

# Define the main function that performs the complete transformation
def youtube_html_transformer():
    """
    Serves as the main entry point for transforming raw YouTube activity data from HTML format
    into a structured DataFrame, which is then saved as an Excel file.

    Returns:
        None
    """

    # Initilaise FileManager Class
    file_manager = FileManager()

    # Load the Parsed HTML file
    parsed_html = file_manager.load_file(FileDirectory.RAW_DATA_PATH, Youtube.PARSED_HTML_DATA)

    # Manipulate the DataFrame
    activity_data = manipulate_activity_data(parsed_html)

    # Save the DataFrame as an Excel file
    file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, activity_data, Youtube.CLEAN_PLAYLIST_DATA)

# This block ensures the youtube_transformer function only runs when the script is executed directly
if __name__ == "__main__":
    youtube_html_transformer()
