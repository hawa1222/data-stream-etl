import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from constants import FileDirectory, Google
from utility import redis_manager, s3_manager
from utility.clean_data import CleanData
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

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

    Methods:
        _refresh_token: Refreshes OAuth 2.0 token if necessary.
        _api_call: Makes paginated API calls to YouTube Data API.
        extract_data: Extracts data from specific YouTube API endpoint.
    """

    def __init__(self, token_file, scopes, max_pages):
        """
        Initialises YouTubeExtractor with token file, scopes, and max pages.
        """
        self.token_file = token_file
        self.scopes = scopes
        self.max_pages = max_pages
        self.credentials = self._refresh_token()  # Refresh OAuth 2.0 token
        self.youtube = build("youtube", "v3", credentials=self.credentials)

    def _refresh_token(self):
        """
        Refreshes OAuth 2.0 token if necessary.

        Returns:
            Constructed credentials

        Raises:
            FileNotFoundError: If token file is not found.
            IOError: If there's an error reading or writing token file.
        """
        logger.info("Checking if API token needs refreshing...")

        try:
            # Create Credentials object
            creds = Credentials.from_authorized_user_file(self.token_file, self.scopes)
            if creds.expired and creds.refresh_token:
                logger.warning("Token has expired. Refreshing token...")
                creds.refresh(Request())  # Refresh token
                logger.info("Successfully refreshed token")
                with open(self.token_file, "w") as token:
                    token.write(creds.to_json())  # Save new credentials to token file
                    logger.info("Successfully saved new credentials to token.json")
            else:
                logger.info("Token is still valid. No need to refresh")

            return creds

        except FileNotFoundError as e:
            logger.error(f"FileNotFoundError Error refreshing access_token: {str(e)}")
            raise

        except IOError as e:
            logger.error(f"I/O Error refreshing access_token: {str(e)}")
            raise

    def _api_call(self, api_endpoint, api_params):
        """
        Makes paginated API calls to YouTube Data API.

        Parameters:
            api_endpoint: Type of API call (e.g., 'channels', 'playlistItems').
            api_params: Parameters for API call (e.g., 'part', 'id').

        Returns:
            list: Raw data entries from API.

        Raises:
            HttpError: If there's an error with API request.
            Exception: If there's an unexpected error.
        """
        items = []
        next_page_token = None  # Token for fetching next page of data
        for page in range(self.max_pages):
            try:
                if next_page_token:  # Add token for next page if available
                    api_params["pageToken"] = next_page_token
                response = (
                    getattr(self.youtube, api_endpoint)().list(**api_params).execute()
                )  # Make API call
                items.extend(response.get("items", []))  # Add items to list
                next_page_token = response.get("nextPageToken")  # Get next page token
                logger.info(f"Fetched page {page + 1} of {api_endpoint} data")

                if not next_page_token:
                    break  # Break loop if no more pages to fetch

            except HttpError as e:
                logger.error(f"HTTP Error making API call to {api_endpoint}: {str(e)}")
                raise

            except Exception as e:
                logger.error(f"Error making API call to {api_endpoint}: {str(e)}")
                raise

        return items

    def extract_data(self, config):
        """
        Extracts data from specific YouTube API endpoint.

        Parameters:
            config: Configuration for API call, including endpoint and parameters.

        Returns:
            list: Raw data entries from API.

        Raises:
            RuntimeError: If API call fails or returns unexpected data.
            Exception: If there's an unexpected error.
        """
        api_endpoint = config["api_endpoint"]  # Extract API endpoint
        params = config["parameters"]  # Extract API parameters

        logger.info(f"Fetching YouTube API data from {api_endpoint} endpoint...")
        try:
            raw_data = self._api_call(api_endpoint, params)  # Make API call

            if not raw_data:  # No data returned
                raise RuntimeError

            logger.info(f"Successfully fetched {len(raw_data)} total entries")

            return raw_data

        except RuntimeError as e:
            logger.error(f"Error, no data returned: {str(e)}")
            raise

        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise


def clean_desc(df, field_name):
    """
    Cleans description field in DataFrame.

    Replaces "http://" and "https://" with "http[://]" and "https[://]" respectively.
    Adds single quote "'" to beginning of string if it starts with "=".

    Parameters:
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
    Main function to fetch YouTube Data, drop NAs, standardise field names, update cache,
    save to S3 and local storage.
    """
    logger.info("!!!!!!!!!!!! youtube_extractor.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()
        extractor = YouTubeExtractor(Google.TOKEN_FILE, Google.SCOPES, max_pages=200)

        # Fetch data for each API endpoint ('subscriptions', 'channels', 'playlistItems')
        for data_name, config in Google.API_CONFIG.items():
            obj_key = f"youtube_{data_name}"  # Define object key

            cached_data = redis_manager.get_cached_data(obj_key)  # Fetch cached data
            if cached_data is not None:  # Check cached data exists
                sd_data = pd.DataFrame(cached_data)  # Convert to DataFrame
                logger.info(f"Successfully fetched {len(sd_data)} total entries")
            else:
                raw_data = extractor.extract_data(config)  # Fetch new data

                # Flatten JSON data, standardise, and clean description field
                normalised_data = pd.json_normalize(raw_data)
                sd_data = CleanData.clean_data(normalised_data, 5)
                sd_data = clean_desc(sd_data, "snippet.description")

                # Cache new data, upload to S3
                redis_manager.update_cached_data(obj_key, sd_data)
                s3_manager.post_data_to_s3(obj_key, sd_data, overwrite=True)

            file_manager.save_file(FileDirectory.RAW_DATA_PATH, obj_key, sd_data)

    except Exception as e:
        logger.error(f"Error occurred in youtube_extractor: {str(e)}")


if __name__ == "__main__":
    youtube_extractor()
