"""
This script is responsible for extracting data from the YouTube API, particularly channel, playlist, and subscription data.
It also performs data cleaning and token refreshing as needed.

Key Processes:
1. YouTube API Data Extraction:
   - Calls the YouTube API to fetch data based on specified API types and parameters.
   - Handles pagination to retrieve multiple pages of data.

2. Data Cleaning:
   - Cleans and sanitises the fetched data, ensuring compatibility with Excel and handling special characters.
   - Applies text cleaning to the description column.

3. Token Refreshing:
   - Checks if the Google OAuth2 token is expired and refreshes it if necessary.

4. Data Saving:
   - Saves the extracted and cleaned data in Excel format.

Usage:
- Execute this script as the main module to extract YouTube data using the YouTube API.
- Ensure that Google OAuth2 credentials and tokens are correctly configured.
- The script assumes that the token needs to be refreshed when it's expired.

Note:
- This script is part of a larger data processing system for managing YouTube data.
- It ensures that YouTube data is extracted, cleaned, and stored for further analysis or reporting.
"""

# Import standard libraries
import re  # For regular expressions
from importlib import reload  # For reloading modules
from dotenv import set_key  # For setting environment variables

# Import Google API libraries
from google.oauth2.credentials import Credentials  # For managing OAuth2 credentials
from google.auth.transport.requests import Request  # For making HTTP requests in the Google Auth process
from googleapiclient.discovery import build  # For building the Google API client

# Import custom utility modules
from utility.logging import setup_logging  # Custom logging setup
from utility.file_manager import FileManager  # Custom file management
from utility.standardise_fields import DataStandardiser  # Custom data standardisation
from utility.cache_data import initialise_cache, update_cache  # Custom cache initialisation and updating

# Import custom configuration and constants
import config  # Custom configuration
from config import (GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_ACCESS_TOKEN,
                    GOOGLE_REFRESH_TOKEN, GOOGLE_TOKEN_EXPIRY)
from constants import FileDirectory, Youtube

# Initialise logging
logger = setup_logging()

#############################################################################################

def refresh_google_token(token, refresh_token, client_id, client_secret, token_url, expiry):
    """
    Refreshes Google OAuth2 credentials using a refresh token.

    Parameters:
    - token (str): The existing access token.
    - refresh_token (str): The refresh token for obtaining new access tokens.
    - client_id (str): The client ID from Google Developer Console.
    - client_secret (str): The client secret from Google Developer Console.
    - token_url (str): The token URI for Google's OAuth2.
    - expiry (str): The expiry time for the current access token.

    Returns:
    - Credentials: The updated Google OAuth2 credentials.
    """
    # Check if credentials need refreshing
    logger.info("Checking if credentials need refreshing...")
    credentials = Credentials.from_authorized_user_info(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "token_url": token_url,
            "token": token,
            'expiry': expiry
        },
        scopes=Youtube.SCOPES
    )
    
    # Code for refreshing the credentials if they are expired
    if credentials.expired:
        logger.info("Refreshing credentials...")
        credentials.refresh(Request())
        # Extract new token details
        logger.info("Extracting new token details...")
        new_access_token = credentials.token
        new_refresh_token = credentials.refresh_token
        new_token_expiry = credentials.expiry
        formatted_expiry = ''
        if new_token_expiry is not None:
            formatted_expiry = new_token_expiry.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        logger.info("New access token received.")
        # Update environment variables with new token details
        set_key(FileDirectory.ENV_PATH, 'GOOGLE_ACCESS_TOKEN', str(new_access_token))
        set_key(FileDirectory.ENV_PATH, 'GOOGLE_TOKEN_EXPIRY', formatted_expiry)
        # If a new refresh token is received, update it
        if new_refresh_token != refresh_token:
            set_key(FileDirectory.ENV_PATH, 'GOOGLE_REFRESH_TOKEN', str(new_refresh_token))
            logger.info("New refresh token received.")
    else:
        logger.info('Credentials are still valid')

    # Reload the config to update environment variables
    reload(config)

    return credentials

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
        youtube = build('youtube', 'v3', credentials=credentials)
        # Set next page token for pagination
        if next_page_token:
            params['pageToken'] = next_page_token
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
        logger.info(f"Fetched page {page} data for {api_type}.")

        # Break if no more pages
        if next_page_token is None:
            break

    # Standardise the fetched API data into a DataFrame
    standardiser = DataStandardiser()
    api_data_df = standardiser.json_normalise(api_data, one_level_above=True)

    # Apply text cleaning to description column
    api_data_df[Youtube.DESC] = api_data_df[Youtube.DESC].apply(clean_description)
    logger.info(f"Cleaned description column for {api_type}.")

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
    s = re.sub(r'[^a-zA-Z0-9\s\[\]\:\-\_\,\.\!\/\@\#\$\%\^\&\*\(\)\+\=\;\:\'\"\?\>\<\~\`]', '', s)
    # Remove non-printable characters
    s = re.sub(r'[^\x20-\x7E]', '', s)
    # Truncate text to fit within Excel's cell limit
    if len(s) > 32767:
        s = s[:32767]
    # Handle Excel-specific character quirks
    if s.startswith('='):
        s = "'" + s
    # Replace line breaks for readability
    s = s.replace('\n', ', ').replace('\r', ', ')

    return s

#############################################################################################

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
    
    # Initilaise FileManager Class
    file_manager = FileManager()

    # Check if the token is expired and refresh it if necessary
    try:
        credentials = refresh_google_token(GOOGLE_ACCESS_TOKEN, GOOGLE_REFRESH_TOKEN,
                                           GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET,
                                           Youtube.TOKEN_URL, GOOGLE_TOKEN_EXPIRY)
    except Exception as e:
        logger.error("Error refreshing Google API token: {}".format(str(e)))
        return None

    # Get the API response for channels
    channel_response = youtube_api(credentials, Youtube.CHANNEL_API_CALL, Youtube.CHANNEL_API_PARAMS)

    # Get the API response for playlistItems (liked videos)
    playlist_response = youtube_api(credentials, Youtube.PLAYLIST_API_CALL, Youtube.LIKES_API_PARAMS)
    playlist_response[Youtube.PLAYLIST] = Youtube.PLAYLIST_VALUE[0]
    playlist_response[Youtube.SOURCE] = Youtube.SOURCE_VALUE[0]
    
    # Load & update the cached data
    likes_cache = initialise_cache(FileDirectory.RAW_DATA_PATH, Youtube.CACHE_LIKES_DATA)
    updated_likes_cache = update_cache(FileDirectory.RAW_DATA_PATH, likes_cache, playlist_response,
                                       Youtube.CACHE_LIKES_DATA, Youtube.LEGACY_VID_ID)

    # Get the API response for subscriptions
    subscription_response = youtube_api(credentials, Youtube.SUBS_API_CALL, Youtube.SUBS_API_PARAMS)

    # Save API data in Excel format
    file_manager.save_file(FileDirectory.RAW_DATA_PATH, channel_response, Youtube.CHANNEL_DATA)
    file_manager.save_file(FileDirectory.RAW_DATA_PATH, subscription_response, Youtube.SUBS_DATA)

# Run the main function
if __name__ == "__main__":
    youtube_extractor()
