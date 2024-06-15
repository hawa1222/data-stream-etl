"""
YouTube Activity Data Extraction

This script is responsible for parsing and extracting activity data about liked and disliked videos on YouTube from an HTML file.
It utilises BeautifulSoup to parse the HTML and extracts relevant information.

Key Functions:
1. `parse_activity_data(html_file)`:
   - Parses an HTML file to extract data about liked and disliked videos on YouTube.
   - Returns a DataFrame containing the parsed activity data.

2. `youtube_html_extractor()`:
   - Main function that loads the YouTube HTML file, parses it, and saves the parsed data.

Usage:
- Execute this script as the main module to extract activity data from a YouTube HTML file.
- Ensure that the HTML file containing YouTube activity data is available in the specified directory.

Note:
- This script is designed to work with YouTube activity data exported manually from YouTube.
- It extracts information such as the date of activity, whether a video was liked or disliked, video titles, video URLs, channel titles, and channel URLs.
- The parsed data is saved in a structured format for further analysis or reporting.
- It is part of a larger data processing system for managing YouTube data.
"""

# Import required libraries
import pandas as pd

# Custom imports
from constants import FileDirectory, Youtube
from utility.file_manager import FileManager
from utility.logging import setup_logging  # Custom logging setup

# Initialise logging
logger = setup_logging()

#############################################################################################


# Define a function for parsing activity data from an HTML file
def parse_activity_data(html_file):
    """
    Parses an HTML file to extract data about liked and disliked videos on YouTube.

    Parameters:
        html_file (BeautifulSoup): A BeautifulSoup object containing the parsed HTML file.

    Returns:
        DataFrame: A dataframe containing the parsed activity data.
    """

    # Log the start of the parsing process
    logger.info("Parsing HTML data...")

    # Find all div elements that match the specified class - these contain the activity data
    liked_video_elements = html_file.find_all(
        "div", {"class": "content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"}
    )

    # Initialise an empty list to store each video's activity data
    activity_data = []

    # Iterate through each div element found
    for element in liked_video_elements:
        # Create a dictionary to store data for this specific video activity
        activity = {}

        # Extract the text content of the current div element
        element_text = element.get_text()

        # Check if the text indicates a 'Liked' or 'Disliked' video
        if element_text.startswith(
            Youtube.PLAYLIST_VALUE[0]
        ) or element_text.startswith(Youtube.PLAYLIST_VALUE[1]):
            # Extract the date of the activity and store it in the dictionary
            activity[Youtube.DATE] = element.contents[-1].strip()

            # Determine if the video was liked or disliked and store this information
            activity[Youtube.PLAYLIST] = (
                Youtube.PLAYLIST_VALUE[0]
                if Youtube.PLAYLIST_VALUE[0] in element_text
                else Youtube.PLAYLIST_VALUE[1]
            )

            # Find all anchor (a) tags within the div element - these usually contain hyperlinks
            a_tags = element.find_all("a")

            # If there's at least one anchor tag, extract the video title and URL
            if len(a_tags) > 0:
                activity[Youtube.VID_TITLE] = a_tags[
                    0
                ].text  # Video title is in the first anchor tag
                activity[Youtube.VID_URL] = a_tags[0][
                    "href"
                ]  # Video URL is the 'href' attribute of the first anchor tag

            # If there are at least two anchor tags, extract the channel title and URL
            if len(a_tags) > 1:
                activity[Youtube.VID_OWNER] = a_tags[
                    1
                ].text  # Channel title is in the second anchor tag
                activity[Youtube.CHANNEL_URL] = a_tags[1][
                    "href"
                ]  # Channel URL is the 'href' attribute of the second anchor tag

            # Add the dictionary with the video's activity data to the list
            activity_data.append(activity)

    # Convert the list of video activity dictionaries to a DataFrame
    activity_data_df = pd.DataFrame(activity_data)

    # Log the successful completion of the parsing process
    logger.info("Successfully parsed HTML data")

    # Return the DataFrame containing the activity data
    return activity_data_df


#############################################################################################


# Function responsible for extracting data
def youtube_html_extractor():
    """
    Load & Parse Youtube HTML Data
    """
    # Initilaise FileManager Class
    file_manager = FileManager()

    # Load youtube HTML file from iCloud
    html_data = file_manager.load_file(
        FileDirectory.MANUAL_EXPORT_PATH, Youtube.RAW_HTML_DATA
    )

    # Extract data from HTML file
    parsed_data = parse_activity_data(html_data)

    # Save Data
    file_manager.save_file(
        FileDirectory.RAW_DATA_PATH, parsed_data, Youtube.PARSED_HTML_DATA
    )


if __name__ == "__main__":
    # Execute the data extraction function
    youtube_html_extractor()
