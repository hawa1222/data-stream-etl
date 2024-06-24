import sys
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
from constants import APIHandler, FileDirectory, StravaAPI
from utility import cache_data, download_data_local, upload_data_s3
from utility.logging import setup_logging
from utility.utils import exception_formatter

# Initialise logging
logger = setup_logging()

# Setting up headers for API requests
auth_headers = {"Authorization": f"Bearer {STRAVA_ACCESS_TOKEN}"}


def refresh_access_token(token_url, client_id, client_secret, refresh_token):
    """
    Refreshes access token using provided refresh token.

    Args:
        token_url: URL endpoint for refreshing access token.
        client_id: Client ID for authentication.
        client_secret: Client secret for authentication.
        refresh_token: Refresh token used to obtain a new access token.

    Returns:
        str: New access token if refresh is successful.

    Raises:
        HTTPError: If response status code is 4xx or 5xx.
        Exception: If error occurs during access token refresh process.

    """

    logger.info("Access token expired. Attempting to update access_token...")

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }

    try:
        response = requests.post(token_url, data=payload)
        response.raise_for_status()  # Raise exception for 4xx or 5xx status codes
        json_response = response.json()

        new_access_token = json_response.get("access_token")
        set_key(FileDirectory.ENV_PATH, "STRAVA_ACCESS_TOKEN", new_access_token)
        logger.info("Successfully updated access_token")

        auth_headers["Authorization"] = f"Bearer {new_access_token}"
        logger.info("Succesfully updated authisation_header")

        new_refresh_token = json_response.get("refresh_token", refresh_token)
        if new_refresh_token != refresh_token:
            set_key(FileDirectory.ENV_PATH, "STRAVA_REFRESH_TOKEN", new_refresh_token)
            logger.info("Successfully updated refresh_token")

        reload(config)

        return True

    except requests.exceptions.HTTPError as e:
        logger.error(
            f"Exiting script due to error: {exception_formatter(e, 'requests')}"
        )
        sys.exit(1)  # Exit script if HTTP error occurs

    except Exception as e:
        logger.error(f"Exiting script due to error: {exception_formatter(e)}")
        sys.exit(1)  # Exit script if error occurs


def api_error_handler(status_code, activity_id=None):
    """
    Handle rate limits, token expiration and other API errors when interacting with Strava API.

    Args:
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
                f"Rate limit exceeded. Sleeping for {APIHandler.RATE_LIMIT_SLEEP_TIME/60} minutes..."
            )
            time.sleep(APIHandler.RATE_LIMIT_SLEEP_TIME)
            logger.info("Resuming API request after rate limit sleep")

            return True

        if status_code == APIHandler.HTTP_401_UNAUTHORISED:
            return refresh_access_token(
                StravaAPI.TOKEN_URL,
                STRAVA_CLIENT_ID,
                STRAVA_CLIENT_SECRET,
                STRAVA_REFRESH_TOKEN,
            )

        if status_code == APIHandler.HTTP_404_NOT_FOUND:
            logger.warning(f"Activity {activity_id} not found")

            return False

        if 400 <= status_code < 600:
            raise requests.exceptions.HTTPError

    except requests.exceptions.HTTPError as e:
        logger.error(f"Exiting script due to error: {exception_formatter(e)}")
        sys.exit(1)  # Exit script if HTTP error occurs

    except Exception as e:
        logger.error(f"Exiting script due to error: {exception_formatter(e)}")
        sys.exit(1)  # Exit script if error occurs

    return False


def get_activity_ids(headers):
    """
    Fetch all activity ids from Strava API.

    Args:
        headers: Dictionary containing Authorisation header.

    Returns:
        set: A set containing all unique activity_id values.
    """

    current_datetime = datetime.now().strftime(Settings.DATETIME_FORMAT)
    logger.info(
        f"Fetching all activity IDs as of {current_datetime} from Strava API..."
    )

    page = 1
    all_activity_ids = set()

    while True:
        response = requests.get(
            StravaAPI.BASE_URL,
            headers=headers,
            params={"page": page, "per_page": APIHandler.ITEMS_PER_PAGE},
        )
        if response.status_code != APIHandler.HTTP_200_OK:
            api_error_handler(response.status_code)
            continue  # Retry request for same page

        activities = response.json()

        if not activities:
            break  # Exit loop when no more activities available

        activity_ids = {str(activity["id"]) for activity in activities}
        all_activity_ids.update(activity_ids)  # Add new activity_ids to set

        logger.info(f"Fetched page {page} of activity_ids")
        page += 1

        if len(activities) < APIHandler.ITEMS_PER_PAGE:
            break  # Exit loop if less than 50 activities on page

    logger.info(f"Successfully fetched {len(all_activity_ids)} activity IDs")

    return all_activity_ids


def get_activity_data(new_activity_ids, headers):
    """
    Fetch activity data for a list of activities from the Strava API.

    Args:
        new_activity_ids: List of new activity IDs.
        headers: Dictionary containing Authorisation header.

    Returns:
        list: List containing detailed data for each activity in JSON format.
    """

    logger.info("Fetching complete data for new activity IDs from Strava API...")

    activity_data_json = []

    for activity_id in new_activity_ids:
        while True:
            url = StravaAPI.ACTIVITY_URL.format(activity_id)
            params = {"include_all_efforts": False}
            response = requests.get(url, headers=headers, params=params, timeout=10)

            if response.status_code != APIHandler.HTTP_200_OK:
                api_error_handler(response.status_code, activity_id)
                continue  # Retry request for same page

            activity_data_json.append(response.json())
            logger.info(
                "Successfully fetched activity data for activity %s",
                activity_id,
            )
            break  # Exit while loop when request is successful

    logger.info(f"{len(activity_data_json)} new activities data fetched")

    return activity_data_json


def strava_extractor():
    """
    Main function to fetch cached activity IDs, extract all activity IDs from Strava API,
    isolate new activity IDs, fetch detailed data for new activities, flatten JSON data,
    save data to S3, update local copy, and add new activity IDs to cache.
    """
    try:
        # Initialise cache and get cached activity IDs from Redis
        cached_ids = cache_data.get_cached_ids("strava_activity_ids")

        # Fetch all activity IDs
        all_activity_ids = get_activity_ids(auth_headers)

        # Filter out activity_ids not in cache_ids
        new_activity_ids = all_activity_ids - cached_ids

        if len(new_activity_ids) > 0:
            logger.info(
                f"{len(new_activity_ids)} new activity IDs found in Strava API Data: {new_activity_ids}"
            )

            # Fetch activities data for new_activity_ids
            new_activity_data = get_activity_data(new_activity_ids, auth_headers)

            # Flatten JSON data intoDataFrame
            new_activity_data_df = pd.json_normalize(new_activity_data)

            # Save data to S3
            upload_data_s3.post_data_to_s3(new_activity_data_df, "strava_activity_data")

            # Update local copy
            download_data_local.update_local_data(
                FileDirectory.RAW_DATA_PATH,
                StravaAPI.COMPLETE_DATA,
                new_activity_data_df,
            )

            # Cache new activity IDs in Redis
            cache_data.cache_ids("strava_activity_ids", new_activity_ids)
        else:
            logger.info("No new activities found in Strava API")

    except Exception as e:
        logger.error(f"{exception_formatter(e)}")


if __name__ == "__main__":
    strava_extractor()
