import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from constants import FileDirectory, Google
from utility import cache_data, standardise_data, upload_data_s3
from utility.file_manager import FileManager
from utility.logging import setup_logging

logger = setup_logging()


class YouTubeExtractor:
    """
    Handles YouTube data extraction using YouTube Data API.

    Manages API authentication, token refreshing, and data extraction
    for YouTube API endpoints.

    Attributes:
        token_file: Path to token file for API authentication.
        scopes: List of API scopes required for authentication.
        max_pages: Maximum number of pages to fetch from API.
        credentials: OAuth 2.0 credentials.
        youtube: YouTube API service object.
    """

    def __init__(self, token_file, scopes, max_pages):
        """
        Initialises YouTubeExtractor with token file, scopes, and max pages.
        """
        self.token_file = token_file
        self.scopes = scopes
        self.max_pages = max_pages

        try:
            self.credentials = self._refresh_token()
            self.youtube = build("youtube", "v3", credentials=self.credentials)
        except Exception as e:
            logger.error(f"Failed to initialise YouTubeExtractor: {str(e)}")
            raise

    def _refresh_token(self):
        """
        Refreshes OAuth 2.0 token if necessary.

        Returns:
            Refreshed credentials.

        Raises:
            FileNotFoundError: If token file is not found.
            IOError: If there's an error reading or writing token file.
        """
        logger.info("Checking if API token needs refreshing...")

        try:
            # Load credentials from token file into Credentials object
            creds = Credentials.from_authorized_user_file(self.token_file, self.scopes)
            if creds.expired and creds.refresh_token:  # Check if token expired
                logger.warning("Token has expired. Refreshing token...")
                creds.refresh(Request())  # Refresh token
                logger.info("Successfully refreshed token")
                with open(self.token_file, "w") as token:
                    token.write(creds.to_json())  # Save new credentials to token file
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

    def _api_call(self, api_endpoint, params):
        """
        Makes paginated API calls to YouTube Data API.

        Args:
            api_endpoint: Type of API call (e.g., 'channels', 'playlistItems').
            params: Parameters for API call (e.g., 'part', 'id').

        Returns:
            List of items returned by API.

        Raises:
            HttpError: If there's an error with API request.
            Exception: If there's an unexpected error.
        """
        items = []
        next_page_token = None  # Token for fetching next page of data
        for page in range(self.max_pages):
            try:
                if next_page_token:  # Add token for next page if available
                    params["pageToken"] = next_page_token
                response = (
                    getattr(self.youtube, api_endpoint)().list(**params).execute()
                )  # Make API call
                items.extend(response.get("items", []))  # Add items to list
                next_page_token = response.get("nextPageToken")  # Get next page token
                logger.info(f"Fetched page {page + 1} of {api_endpoint} data")
                if not next_page_token:
                    break  # Break loop if no more pages to fetch
            except HttpError as e:
                logger.error(f"An HTTP error occurred: {e}")
                raise
            except Exception as e:
                logger.error(f"An error occurred: {e}")
                raise
        return items

    def extract_data(self, config):
        """Extracts data from specific YouTube API endpoint.

        Args:
            config: Configuration for API call, including endpoint and parameters.

        Returns:
            List of raw data entries from API.

        Raises:
            RuntimeError: If API call fails or returns unexpected data.
            Exception: If there's an unexpected error.
        """
        api_endpoint = config["api_endpoint"]  # Extract API endpoint
        params = config["parameters"]  # Extract API parameters

        logger.info(f"Fetching YouTube API data from {api_endpoint} endpoint...")
        try:
            raw_data = self._api_call(api_endpoint, params)  # Make API call
            if not raw_data:  # Check if data returned
                error_message = f"No data returned from {api_endpoint} endpoint"
                logger.error(error_message)
                raise RuntimeError(error_message)
            logger.info(f"Successfully fetched {len(raw_data)} total entries")
            return raw_data
        except Exception as e:
            error_message = (
                f"Error occured extracting data from {api_endpoint} endpoint: {str(e)}"
            )
            logger.error(error_message)
            raise Exception(error_message)


def clean_desc(df, field_name):
    """
    Cleans description field in DataFrame.

    Replaces "http://" and "https://" with "http[://]" and "https[://]" respectively.
    Adds single quote "'" to beginning of string if it starts with "=".

    Args:
        df: DataFrame to clean.
        field_name: name of field to clean.

    Returns:
        cleaned DataFrame.
    """
    df[field_name] = (
        df[field_name]
        .str.replace("http://", "http[://]")
        .str.replace("https://", "https[://]")
    )
    df[field_name] = df[field_name].apply(lambda s: "'" + s if s.startswith("=") else s)

    return df


def youtube_extractor():
    """
    Main function to fetch & standardise ouTube data, update cache, save data to S3,
    and local storage.
    """
    logger.info("!!!!!!!!!!!! youtube_extractor.py !!!!!!!!!!!")

    try:
        # Initialise FileManager & YouTubeExtractor
        file_manager = FileManager()
        extractor = YouTubeExtractor(Google.TOKEN_FILE, Google.SCOPES, max_pages=200)

        # Fetch data for each API endpoint ('subscriptions', 'channels', 'playlistItems')
        for data_name, config in Google.API_CONFIG.items():
            cache_key = f"youtube_{data_name}"  # Define cache key

            cached_data = cache_data.get_cached_data(cache_key)  # Get cached data
            if cached_data is not None:  # Check if cached data exists
                raw_data = cached_data  # Use cached data
                logger.info(f"Successfully fetched {len(raw_data)} total entries")
            else:
                raw_data = extractor.extract_data(config)  # Fetch new data

                cache_data.update_cached_data(cache_key, raw_data)  # Cache new data

                normalised_data = pd.json_normalize(raw_data)  # Flatten JSON data
                # Standardise data & clean description field
                standardised_data = standardise_data.CleanData.clean_data(
                    normalised_data, na_threshold=5
                )
                standardised_data = clean_desc(standardised_data, "snippet.description")

                upload_data_s3.post_data_to_s3(
                    standardised_data, cache_key, overwrite=True
                )  # Upload data to S3, overwrite existing data
                file_manager.save_file(
                    FileDirectory.RAW_DATA_PATH,
                    standardised_data,
                    cache_key + "_data.xlsx",
                )  # Save data to local file
    except Exception as e:
        logger.error(f"Error occurred fetching YouTube data: {str(e)}")


if __name__ == "__main__":
    youtube_extractor()
