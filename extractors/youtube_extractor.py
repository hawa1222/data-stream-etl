import os
import re

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from constants import FileDirectory, Youtube
from utility.download_data_local import (
    initialise_cache,
    update_cache,
)
from utility.file_manager import FileManager
from utility.logging import setup_logging
from utility.standardise_fields import DataStandardiser

# Initialise logging
logger = setup_logging()


token_file = os.path.join(FileDirectory.ROOT_DIRECTORY, "credentials/google/token.json")


def refresh_token():
    logger.info("Checking if token needs refreshing...")

    creds = Credentials.from_authorized_user_file(token_file, Youtube.SCOPES)

    if creds.expired:
        logger.info("Token has expired. Refreshing token...")
        creds.refresh(Request())
        logger.info("Successfully refreshed token")

        # Save the credentials for the next run
        with open(token_file, "w") as token:
            token.write(creds.to_json())
            logger.info("Successfully saved new credentials to token.json")

    else:
        logger.info("Token is still valid. No need to refresh")

    return creds


def youtube_api(credentials, api_type, params, max_pages=200):
    """
    Fetches data from the YouTube API based on the type of API and parameters.

    Parameters:
    - credentials (Credentials): The Google OAuth2 credentials.
    - api_type (str): The type of YouTube API to call ('playlistItems', 'channels', etc.).
    - params (dict): Parameters to pass into the API call.
    - max_pages (int): The maximum number of API pages to fetch.

    Returns:
    - DataFrame: A DataFrame containing the standardised API response data.
    """
    # Initialise data container and pagination variables
    api_data = []
    next_page_token = None
    page = 0

    # Loop for pagination through API
    while True:
        # Break if maximum number of pages reached
        if page >= max_pages:
            break
        # Build the YouTube API client
        youtube = build("youtube", "v3", credentials=credentials)
        # Set next page token for pagination
        if next_page_token:
            params["pageToken"] = next_page_token
        # Dynamically fetch the correct API function
        api_function = getattr(youtube, api_type)().list(**params)
        # Make the API call and fetch the response
        try:
            api_response = api_function.execute()
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            break

        # Loop through each item in the API response to extract details
        for item in api_response.get("items", []):
            api_data.append(item)

        # Increase the page counter
        page += 1
        # Fetch the next page token for pagination
        next_page_token = api_response.get("nextPageToken")
        logger.info(f"Fetched page {page} data for {api_type}")

        # Break if no more pages
        if next_page_token is None:
            break

    # Standardise the fetched API data into a DataFrame
    standardiser = DataStandardiser()
    api_data_df = standardiser.json_normalise(api_data, one_level_above=True)

    # Apply text cleaning to description column
    api_data_df[Youtube.DESC] = api_data_df[Youtube.DESC].apply(clean_description)
    logger.info(f"Cleaned description column for {api_type}")

    return api_data_df


def clean_description(s):
    """
    Cleans and sanitises a string, particularly for Excel compatibility.

    Parameters:
    - s (str): The string to be cleaned.

    Returns:
    - str: The cleaned string.
    """
    # Convert to string just in case
    s = str(s)
    # Replace URL prefixes
    s = s.replace("http://", "http[://]").replace("https://", "https[://]")
    # Remove invalid characters using regular expressions
    s = re.sub(
        r"[^a-zA-Z0-9\s\[\]\:\-\_\,\.\!\/\@\#\$\%\^\&\*\(\)\+\=\;\:\'\"\?\>\<\~\`]",
        "",
        s,
    )
    # Remove non-printable characters
    s = re.sub(r"[^\x20-\x7E]", "", s)
    # Truncate text to fit within Excel's cell limit
    if len(s) > 32767:
        s = s[:32767]
    # Handle Excel-specific character quirks
    if s.startswith("="):
        s = "'" + s
    # Replace line breaks for readability
    s = s.replace("\n", ", ").replace("\r", ", ")

    return s


# Main function for extracting data
def youtube_extractor():
    """
    Main function for extracting data from the Google API. This function fetches channel, playlist,
    and subscription data from YouTube API, refreshes the Google OAuth2 token if needed, and saves the data.

    Parameters:
        None

    Returns:
        None
    """

    # Initialise FileManager Class
    file_manager = FileManager()

    credentials = refresh_token()

    # Get the API response for channels
    channel_response = youtube_api(
        credentials, Youtube.CHANNEL_API_CALL, Youtube.CHANNEL_API_PARAMS
    )

    # Get the API response for playlistItems (liked videos)
    playlist_response = youtube_api(
        credentials, Youtube.PLAYLIST_API_CALL, Youtube.LIKES_API_PARAMS
    )
    playlist_response[Youtube.PLAYLIST] = Youtube.PLAYLIST_VALUE[0]
    playlist_response[Youtube.SOURCE] = Youtube.SOURCE_VALUE[0]

    # Load & update the cached data
    likes_cache = initialise_cache(
        FileDirectory.RAW_DATA_PATH, Youtube.CACHE_LIKES_DATA
    )
    updated_likes_cache = update_cache(
        FileDirectory.RAW_DATA_PATH,
        likes_cache,
        playlist_response,
        Youtube.CACHE_LIKES_DATA,
        Youtube.LEGACY_VID_ID,
    )

    # Get the API response for subscriptions
    subscription_response = youtube_api(
        credentials, Youtube.SUBS_API_CALL, Youtube.SUBS_API_PARAMS
    )

    # Save API data in Excel format
    file_manager.save_file(
        FileDirectory.RAW_DATA_PATH, channel_response, Youtube.CHANNEL_DATA
    )
    file_manager.save_file(
        FileDirectory.RAW_DATA_PATH, subscription_response, Youtube.SUBS_DATA
    )


# Run the main function
if __name__ == "__main__":
    youtube_extractor()
