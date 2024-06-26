import os

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from constants import FileDirectory, Youtube
from utility import cache_data, upload_data_s3
from utility.file_manager import FileManager
from utility.logging import setup_logging

logger = setup_logging()


class YouTubeExtractor:
    """Handles YouTube data extraction using YouTube Data API.

    Manages API authentication, token refreshing, and data extraction
    for various YouTube API endpoints.

    Attributes:
        token_file: Path to token file for API authentication.
        scopes: List of API scopes required for authentication.
        max_pages: Maximum number of pages to fetch from API.
        credentials: OAuth 2.0 credentials.
        youtube: YouTube API service object.
    """

    def __init__(self, token_file, scopes, max_pages=200):
        """Initializes YouTubeExtractor.

        Args:
            token_file: Path to token file for API authentication.
            scopes: List of API scopes required for authentication.
            max_pages: Maximum number of pages to fetch from API. Defaults to 200.

        Raises:
            ValueError: If token_file is empty or scopes list is empty.
        """
        self.token_file = token_file
        self.scopes = scopes
        self.max_pages = max_pages

        try:
            self.credentials = self._refresh_token()
            self.youtube = build("youtube", "v3", credentials=self.credentials)
        except Exception as e:
            logger.error(f"Failed to initialize YouTubeExtractor: {e}")
            raise

    def _refresh_token(self):
        """Refreshes OAuth 2.0 token if necessary.

        Returns:
            Refreshed credentials.

        Raises:
            FileNotFoundError: If token file is not found.
            IOError: If there's an error reading or writing token file.
        """
        logger.info("Checking if API token needs refreshing...")

        try:
            creds = Credentials.from_authorized_user_file(self.token_file, self.scopes)
            if creds.expired and creds.refresh_token:
                logger.warning("Token has expired. Refreshing token...")
                creds.refresh(Request())
                logger.info("Successfully refreshed token")
                with open(self.token_file, "w") as token:
                    token.write(creds.to_json())
                    logger.info("Successfully saved new credentials to token.json")
            else:
                logger.info("Token is still valid. No need to refresh")
            return creds
        except FileNotFoundError:
            logger.error(f"Token file not found: {self.token_file}")
            raise
        except IOError as e:
            logger.error(f"Error reading or writing token file: {e}")
            raise

    def _api_call(self, api_type, params):
        """Makes paginated API calls to YouTube Data API.

        Args:
            api_type: Type of API call (e.g., 'channels', 'playlistItems').
            params: Parameters for API call.

        Returns:
            List of items returned by API.

        Raises:
            HttpError: If there's an error with API request.
            AttributeError: If api_type is invalid.
        """
        items = []
        next_page_token = None
        for page in range(self.max_pages):
            try:
                if next_page_token:
                    params["pageToken"] = next_page_token
                response = getattr(self.youtube, api_type)().list(**params).execute()
                items.extend(response.get("items", []))
                next_page_token = response.get("nextPageToken")
                logger.info(f"Fetched page {page + 1} of {api_type} data")
                if not next_page_token:
                    break
            except HttpError as e:
                logger.error(f"An HTTP error occurred: {e}")
                raise
            except AttributeError:
                logger.error(f"Invalid API type: {api_type}")
                raise
        return items

    def extract_data(self, api_type, params):
        """Extracts data from specific YouTube API endpoint.

        Args:
            api_type: Type of API call (e.g., 'channels', 'playlistItems').
            params: Parameters for API call.

        Returns:
            List of raw data entries from API.

        Raises:
            RuntimeError: If API call fails or returns unexpected data.
            ValueError: If api_type or params are invalid.
        """
        if not api_type or not isinstance(api_type, str):
            raise ValueError("api_type must be non-empty string")
        if not params or not isinstance(params, dict):
            raise ValueError("params must be non-empty dictionary")

        logger.info(f"Fetching YouTube API data from {api_type} endpoint...")
        try:
            raw_data = self._api_call(api_type, params)
            if not raw_data:
                raise RuntimeError(f"No data returned from {api_type} endpoint")
            logger.info(f"Successfully fetched {len(raw_data)} entries")
            return raw_data
        except Exception as e:
            logger.error(f"Failed to extract data from {api_type} endpoint: {e}")
            raise RuntimeError(f"Data extraction failed: {str(e)}")


def clean_desc(df, field_name):
    df[field_name] = (
        df[field_name]
        .str.replace("http://", "http[://]")
        .str.replace("https://", "https[://]")
    )
    df[field_name] = df[field_name].apply(lambda s: "'" + s if s.startswith("=") else s)

    return df


def main():
    file_manager = FileManager()

    max_pages = 200
    max_results = 50

    config = {
        "channels": {
            "api_type": "channels",
            "id": "UCjCBfi9gMp0tFlaDzDiu_iQ",
            # "mine": True,
            "part": "snippet",
            "maxResults": max_results,
        },
        "subscriptions": {
            "api_type": "subscriptions",
            "mine": True,
            "part": "snippet",
            "maxResults": max_results,
        },
        "likes": {
            "api_type": "playlistItems",
            "playlistId": "LLjCBfi9gMp0tFlaDzDiu_iQ",
            "part": "snippet",
            "maxResults": max_results,
        },
    }

    token_file = os.path.join(
        FileDirectory.ROOT_DIRECTORY, "credentials/google/token.json"
    )

    extractor = YouTubeExtractor(token_file, Youtube.SCOPES, max_pages=max_pages)
    logger.info("Fetching YouTube data...")

    for data_type, params in config.items():
        api_type = params.pop("api_type")
        cache_key = f"youtube_{data_type}"

        cached_data = cache_data.get_cached_data(cache_key)
        if cached_data is not None:
            raw_data = cached_data
            logger.info(f"Successfully fetched {len(raw_data)} total entries")
        else:
            raw_data = extractor.extract_data(api_type, params)
            cache_data.update_cached_data(cache_key, raw_data)
            normalised_data = pd.json_normalize(raw_data)
            if api_type == "playlistItems":
                raw_data = clean_desc(normalised_data, "snippet.description")
            upload_data_s3.post_data_to_s3(normalised_data, cache_key, overwrite=True)
            file_manager.save_file(
                FileDirectory.RAW_DATA_PATH, normalised_data, cache_key + "_data.xlsx"
            )


if __name__ == "__main__":
    main()
