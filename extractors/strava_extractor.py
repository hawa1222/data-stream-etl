"""
This script extracts data from the Strava API, compares it with cached data, and updates the cache. It also handles token refresh and rate limiting.

Key Processes:
1. Token Refresh:
   - Attempts to refresh the access token for the Strava API.
   - Uses the provided client ID, client secret, and refresh token.
   - Handles token expiration and updates the environment variables with new tokens.
   - Logs token-related events.

2. API Error Handling:
   - Handles rate limits and token expiration when interacting with the Strava API.
   - Implements rate limiting sleep and token refresh as needed.
   - Logs rate limit exceeded and token refresh events.

3. Fetch Summary Data:
   - Retrieves summary activity data from the Strava API.
   - Uses the provided authorisation headers for API requests.
   - Handles pagination to fetch all available data.
   - Logs data fetching events.

4. Fetch Activity Details:
   - Fetches detailed activity data for a list of activities from the Strava API.
   - Uses the provided authorisation headers for API requests.
   - Handles rate limits, token expiration, and errors.
   - Logs data fetching events.

5. Main Extraction Function:
   - Orchestrates the data extraction process.
   - Loads cached data, fetches new data, and updates the cache.
   - Logs data extraction events and errors.
   - Saves the final updated detailed data to an Excel file.

Usage:
- Execute this script as the main module to extract and update Strava data.
- Ensure that the required environment variables (client ID, client secret, and refresh token) are set.

Note:
- This script is part of a larger data processing system for managing Strava data.
"""

# Import required libraries
import logging
import sys  # For Python interpreter control
import time
import requests
from dotenv import set_key
from importlib import reload

# Configuration
sys.dont_write_bytecode = True  # Prevent Python from writing bytecode files (.pyc)
sys.path.append('/Users/hadid/Projects/ETL')  # Add path to system path

# Custom imports
import config as config
from constants import FileDirectory, StravaAPI
from config import (STRAVA_ACCESS_TOKEN, STRAVA_REFRESH_TOKEN, STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET)
from utility.file_manager import FileManager  # Import your FileManager class here
from utility.standardise_fields import DataStandardiser
from utility.cache_data import initialise_cache, update_cache
from utility.logging import setup_logging  # Custom logging setup

# Call the logging setup function to initialise logging
setup_logging()

# Setting up headers for API requests
StravaAPI.AUTH_HEADER['Authorisation'] = f'Bearer {STRAVA_ACCESS_TOKEN}'

#############################################################################################

def refresh_access_token(token_url, client_id, client_secret, refresh_token):
    """
    Refresh the access token for the Strava API.

    Parameters:
        token_url (str): The URL endpoint to refresh the token.
        client_id (str): The client ID of the API.
        client_secret (str): The client secret of the API.
        refresh_token (str): The current refresh token.

    Returns:
        Tuple[str, str]: New access and refresh tokens.
    """
    # Log an informational message indicating the token refresh attempt.
    logging.info("Attempting to refresh access token...")

    # Prepare the payload for the POST request to refresh the token.
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }

    # Make the POST request to the token URL.
    response = requests.post(token_url, data=payload)

    # Check for a successful response (HTTP 200)
    if response.status_code == StravaAPI.HTTP_200_OK:
        json_response = response.json()  # Parse the JSON response.
        new_access_token = json_response['access_token']  # Extract the new access token.
        new_refresh_token = json_response.get('refresh_token', refresh_token)  # Extract the new refresh token.

        # Log the new access token.
        logging.info('New access token received')

        # Update the environment variables with the new tokens
        set_key(FileDirectory.ENV_PATH, 'STRAVA_ACCESS_TOKEN', new_access_token)
        if new_refresh_token != refresh_token:
            set_key(FileDirectory.ENV_PATH, 'STRAVA_REFRESH_TOKEN', new_refresh_token)
            logging.info(f"Refresh token has changed to: {new_refresh_token}")
        # Reload the configuration to pick up the new tokens
        reload(config)
        return new_access_token, new_refresh_token

    elif response.status_code != StravaAPI.HTTP_200_OK:
        logging.error(f"Error refreshing access token: {response.text}")
        sys.exit(1)

    return None, None

def api_error(status_code, activity_id=None):
    """
    Handle rate limits and token expiration when interacting with the Strava API.

    Parameters:
        status_code (int): The HTTP status code from the API response.
        headers (dict): The HTTP headers that were used in the API request.
        activity_id (int, optional): Identifier for the activity, if available.

    Returns:
        bool: True if the rate limit has been handled and the operation should be retried, False otherwise.
    """
    # Check if the rate limit has been exceeded
    if status_code == StravaAPI.HTTP_429_RATE_LIMITED:
        logging.warning('Rate limit exceeded. Sleeping for 15 minutes...')
        time.sleep(StravaAPI.RATE_LIMIT_SLEEP_TIME)
        return True
    # Check if the token is unauthorised (likely expired)
    elif status_code == StravaAPI.HTTP_401_UNAUTHORIZED:
        # Attempt to refresh the token
        new_token, _ = refresh_access_token(StravaAPI.TOKEN_URL, STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, STRAVA_REFRESH_TOKEN)
        if new_token:
            # Setting up headers for API requests
            StravaAPI.AUTH_HEADER['Authorization'] = f'Bearer {new_token}'
            return True
        else:
            logging.error(f"Error refreshing token when fetching activity {activity_id}.")
            return False
    return False

#############################################################################################

def get_strava_data(headers):
    """
    Fetch summary activity data from Strava API.

    Parameters:
        headers (dict): Dictionary containing the Authorisation header.

    Returns:
        list: A list containing all summary data in JSON format.
    """

    page = 1  # Initialise the page number to 1
    per_page = StravaAPI.ITEMS_PER_PAGE  # Number of items to request per page
    all_activities_json = []  # Initialise an empty list to store all activities


    # Infinite loop to keep fetching data until a stopping condition is met
    while True:
        logging.info(f"Fetching summary data for page {page}...")
        # Make an API request to get the activity data
        response = requests.get(StravaAPI.BASE_URL, headers=headers, params={"page": page, "per_page": per_page})

        # Check for rate limiting and handle it if necessary
        if api_error(response.status_code):
            continue  # Continue to the next iteration of the loop, effectively retrying the API request

        # Check if the API response is anything other than HTTP 200
        elif response.status_code != StravaAPI.HTTP_200_OK:
            logging.error(f"Error fetching summary data: {response.text}")
            break  # Exit the loop if there's an error

        else:
            # Convert the JSON response to a Python list of dictionaries
            activities = response.json()
            # If the list is empty, it means there are no more activities to fetch
            if not activities:
                break

            all_activities_json.extend(activities)

        # Increment the page number for the next API request
        page += 1

    return all_activities_json

def get_strava_activity(new_activity_ids, headers):
    """
    Fetch detailed activity data for a list of activities from the Strava API.

    Parameters:
        new_activity_ids (list): List of new activity IDs.
        headers (dict): Dictionary containing the Authorisation header.

    Returns:
        list: A list containing detailed data for each activity in JSON format.
    """

    detailed_data_json = []  # Initialise an empty list to store detailed data

    for activity_id in new_activity_ids:
        logging.info(f"Fetching detailed data for activity {activity_id}...")
        url = StravaAPI.ACTIVITY_URL.format(activity_id)
        response = requests.get(url, headers=headers)

        if response.status_code == StravaAPI.HTTP_200_OK:
            detailed_data_json.append(response.json())
        elif api_error(response.status_code, activity_id):
            continue  # Skip this iteration and try the next activity ID
        else:
            logging.error("Error fetching detailed data for activity %s: %s", activity_id, response.text)

    return detailed_data_json

#############################################################################################

def strava_extractor():
    """
    Main function to extract data from Strava API,
    compare it with cached data, and update the cache.

    Returns:
        tuple: Two DataFrames representing updated summary and detailed data.
    """
    # Initilaise FileManager Class
    file_manager = FileManager()

    # Create an instance of the DataStandardiser class
    standardiser = DataStandardiser()

    # Load cached data into DataFrames
    cached_summary_data = initialise_cache(FileDirectory.RAW_DATA_PATH, StravaAPI.SUMMARY_CACHE_FILE)
    cached_detailed_data = initialise_cache(FileDirectory.RAW_DATA_PATH, StravaAPI.DETAIL_CACHE_FILE)

    #  Fetch new summary data as JSON
    new_activities_json = get_strava_data(StravaAPI.AUTH_HEADER)
    # Convert the JSON to a DataFrame
    new_activities_df = standardiser.json_normalise(new_activities_json, one_level_above=False)
     
    # Check if the 'id' column exists in the DataFrame and is not empty
    if StravaAPI.ID in cached_summary_data.columns and not cached_summary_data.empty:
        cached_ids = set(cached_summary_data[StravaAPI.ID])
    else:
        # If the column doesn't exist or the DataFrame is empty, use an empty set
        cached_ids = set()


    # Filter out the new activities
    new_activity_ids = [
        activity[StravaAPI.ID] for activity in
        new_activities_json if activity[StravaAPI.ID] not in cached_ids]

    # Fetch detailed data for new activities as JSON
    new_detailed_json = get_strava_activity(new_activity_ids, StravaAPI.AUTH_HEADER)

    # Convert the JSON detailed data to a DataFrame
    new_detailed_df = standardiser.json_normalise(new_detailed_json, one_level_above=True)

    # Update the Excel cache files
    updated_summary_data = update_cache(FileDirectory.RAW_DATA_PATH, cached_summary_data, new_activities_df,
                                        StravaAPI.SUMMARY_CACHE_FILE, StravaAPI.ID)
    updated_detailed_data = update_cache(FileDirectory.RAW_DATA_PATH, cached_detailed_data, new_detailed_df,
                                         StravaAPI.DETAIL_CACHE_FILE, StravaAPI.ID)

    # Save the final, updated detailed data to an Excel file
    file_manager.save_file(FileDirectory.RAW_DATA_PATH, updated_detailed_data, StravaAPI.FINAL_DATA)


if __name__ == "__main__":
    strava_extractor()

