import time
from datetime import datetime
from importlib import reload

import pandas as pd
import requests
from dotenv import set_key

import config
from config import (
    STRAVA_ACCESS_TOKEN,
    STRAVA_CLIENT_ID,
    STRAVA_CLIENT_SECRET,
    STRAVA_REFRESH_TOKEN,
    Settings,
)
from constants import APIHandler, FileDirectory, Strava
from utility import file_manager, redis_manager, s3_manager
from utility.clean_data import CleanData
from utility.log_manager import setup_logging

logger = setup_logging()

# Setting up headers for API requests
auth_headers = {"Authorization": f"Bearer {STRAVA_ACCESS_TOKEN}"}


def refresh_access_token(token_url, client_id, client_secret, refresh_token):
    """
    Refreshes access token using provided refresh token.

    Parameters:
        token_url: URL endpoint for refreshing access token.
        client_id: Client ID for authentication.
        client_secret: Client secret for authentication.
        refresh_token: Refresh token used to obtain a new access token.

    Returns:
        bool: True if access token has been refreshed successfully, False otherwise.

    Raises:
        HTTPError: If response status code is 4xx or 5xx.
        Exception: If error occurs during access token refresh process.
    """

    logger.debug("Access token expired. Attempting to update access_token...")

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }

    try:
        response = requests.post(token_url, data=payload)  # Send POST request
        response.raise_for_status()  # Raise exception for 4xx or 5xx status codes
        json_response = response.json()

        new_access_token = json_response.get("access_token")
        set_key(FileDirectory.ENV_PATH, "STRAVA_ACCESS_TOKEN", new_access_token)
        logger.info("Successfully updated access_token")

        auth_headers["Authorization"] = f"Bearer {new_access_token}"
        logger.debug("Succesfully updated authorisation header")

        new_refresh_token = json_response.get("refresh_token", refresh_token)
        if new_refresh_token != refresh_token:
            set_key(FileDirectory.ENV_PATH, "STRAVA_REFRESH_TOKEN", new_refresh_token)
            logger.debug("Successfully updated refresh_token")

        reload(config)  # Reload config to update access token

        return True

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error refreshing access_token: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error refreshing access_token: {str(e)}")
        raise


def api_error_handler(status_code, activity_id=None):
    """
    Handle rate limits, token expiration and other API errors when interacting with Strava API.

    Parameters:
        status_code: HTTP status code from API response.
        activity_id (optional): Identifier for activity, if available.

    Returns:
        bool: True if rate limit has been handled or token has been refreshed successfully,
        False otherwise.

    Raises:
        HTTPError: If response status code is 4xx or 5xx.
        Exception: If error occurs while handling API error.
    """

    logger.warning(f"Error occurred with status code {status_code}")

    try:
        if status_code == APIHandler.HTTP_429_RATE_LIMITED:
            logger.warning(
                f"Rate limit exceeded. Sleeping for {int(APIHandler.RATE_LIMIT_SLEEP_TIME/60)} minutes..."
            )
            time.sleep(APIHandler.RATE_LIMIT_SLEEP_TIME)  # Sleep for 15 minutes
            logger.debug("Retrying request after rate limit sleep")

            return True

        if status_code == APIHandler.HTTP_401_UNAUTHORISED:
            return refresh_access_token(
                Strava.TOKEN_URL,
                STRAVA_CLIENT_ID,
                STRAVA_CLIENT_SECRET,
                STRAVA_REFRESH_TOKEN,
            )  # Refresh access token

        if status_code == APIHandler.HTTP_404_NOT_FOUND:
            logger.warning(f"Activity {activity_id} not found")

            return False

        if 400 <= status_code < 600:
            raise requests.exceptions.HTTPError

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error handling API error: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error handling API error: {str(e)}")
        raise

    return False


def get_activity_ids(headers):
    """
    Fetch all activity IDs from Strava API.

    Parameters:
        headers: Dictionary containing Authorisation header.

    Returns:
        set: Set containing all unique activity_id values.
    """

    current_datetime = datetime.now().strftime(Settings.DATETIME_FORMAT)
    logger.debug(f"Fetching all activity IDs as of {current_datetime} from Strava API...")

    page = 1
    all_activity_ids = set()

    while True:
        response = requests.get(
            Strava.BASE_URL,
            headers=headers,
            params={"page": page, "per_page": Settings.ITEMS_PER_PAGE},
        )
        if response.status_code != APIHandler.HTTP_200_OK:
            api_error_handler(response.status_code)
            continue  # Retry request for same page

        activities = response.json()

        if not activities:
            break  # Exit loop when no more activities available

        activity_ids = {str(activity["id"]) for activity in activities}
        all_activity_ids.update(activity_ids)  # Add new activity_ids to set

        logger.debug(f"Fetched page {page} of activity_ids")
        page += 1

        if len(activities) < Settings.ITEMS_PER_PAGE:
            break  # Exit loop if less than 50 activities on page

    logger.info(f"Successfully fetched {len(all_activity_ids)} activity IDs from Strava API")

    return all_activity_ids


def get_activity_data(new_activity_ids, headers):
    """
    Fetch activity data for a list of activities from the Strava API.

    Parameters:
        new_activity_ids: List of new activity IDs.
        headers: Dictionary containing Authorisation header.

    Returns:
        list: List containing detailed data for each activity in JSON format.
    """

    logger.debug("Fetching complete data for new activity IDs from Strava API...")

    activity_data_json = []

    for activity_id in new_activity_ids:
        while True:  # Retry request if API error occurs
            url = Strava.ACTIVITY_URL.format(activity_id)
            params = {"include_all_efforts": False}
            response = requests.get(url, headers=headers, params=params, timeout=10)

            if response.status_code != APIHandler.HTTP_200_OK:
                api_error_handler(response.status_code, activity_id)
                continue  # Retry request for same page

            activity_data_json.append(response.json())  # Append JSON data to list
            logger.debug("Successfully fetched activity data for activity %s", activity_id)
            break  # Exit while loop when request is successful

    logger.info(f"{len(activity_data_json)} new detailed activities data fetched from Strava API")

    return activity_data_json


def strava_extractor():
    """
    Main function to fetch cached activity IDs, extract all activity IDs from Strava API,
    isolate new activity IDs, fetch detailed data for new activities, flatten JSON data,
    save data to S3 and local storage, and add new activity IDs to cache.
    """
    logger.info("!!!!!!!!!!!! strava_extractor.py !!!!!!!!!!!")

    try:
        cached_ids = redis_manager.get_cached_ids(Strava.ID_KEY)  # Fetch cached IDs
        all_activity_ids = get_activity_ids(auth_headers)  # Fetch all activity IDs
        new_activity_ids = all_activity_ids - cached_ids  # Filter new activity IDs

        if len(new_activity_ids) > 0:  # Check if new activities found
            logger.info(
                f"{len(new_activity_ids)} new activity IDs found in Strava API Data: {new_activity_ids}"
            )

            new_activity_data = get_activity_data(new_activity_ids, auth_headers)

            new_activity_data_df = pd.json_normalize(new_activity_data)  # Flatten JSON
            new_activity_data_df = CleanData.clean_data(new_activity_data_df, 20)

            # Upload new data to S3, update local copy, and cache new activity IDs
            s3_manager.post_data_to_s3(Strava.DATA_KEY, new_activity_data_df)
            file_manager.update_excel(
                FileDirectory.RAW_DATA_PATH,
                Strava.DATA_KEY,
                new_activity_data_df,
            )
            redis_manager.update_cached_ids(Strava.ID_KEY, new_activity_ids)
            redis_manager.update_cached_data(Strava.DATA_KEY, new_activity_data_df)
        else:
            logger.info("No new activities found in Strava API")

    except Exception as e:
        logger.error(f"Error occurred in strava_extractor: {str(e)}")


if __name__ == "__main__":
    strava_extractor()
