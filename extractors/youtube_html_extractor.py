import re
from collections import Counter
from datetime import datetime

import pandas as pd
from zoneinfo import ZoneInfo

from config import Settings
from constants import FileDirectory, Google
from utility import upload_data_s3
from utility.file_manager import FileManager
from utility.logging import setup_logging

logger = setup_logging()


def parse_date(date_string):
    """
    Parses date string and converts to UTC format.

    Args:
        date_string: date string to be parsed.

    Returns:
        str: parsed date string in UTC format ("%Y-%m-%dT%H:%M:%SZ").

    Example:
        >>> parse_date("Jun 1, 2024, 9:33:45 PM BST")
        '2024-06-01T21:33:45Z'
    """
    uk_tz = ZoneInfo(Settings.TIMEZONE)  # UK timezone

    # Remove leading/trailing whitespace and timezone abbreviations
    date_string = date_string.strip()
    date_string = re.sub(r"\b(BST|GMT|UTC)\b", "", date_string).strip()

    formats = [
        "%b %d, %Y, %I:%M:%S %p",  # Jun 1, 2024, 9:33:45 PM
        "%Y-%m-%d %H:%M:%S",  # 2024-06-01 21:33:45
        "%Y-%m-%dT%H:%M:%S",  # 2024-06-01T21:33:45
    ]

    # Try parse date using each format
    for fmt in formats:
        try:
            dt = datetime.strptime(date_string, fmt)  # Parse date
            dt = dt.replace(tzinfo=uk_tz)  # Set to current UK timezone
            utc_dt = dt.astimezone(ZoneInfo("UTC"))  # Convert to UTC timezone
            return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue

    logger.error(f"Error parsing date '{date_string}")
    return None


def extract_activities(soup):
    """
    Extracts activities from HTML soup.

    Args:
        soup: BeautifulSoup object representing HTML soup.

    Returns:
        list: List of dictionaries representing extracted activities.
    """
    logger.info("Extracting activities from HTML...")

    activities = []

    for div in soup.find_all(
        "div", class_="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"
    ):
        text = div.get_text(strip=True)
        date = div.contents[-1].strip()

        for activity_type in Google.ACTIVITY_TYPES:
            if text.startswith(activity_type):
                activity = {
                    "activity_type": activity_type,
                    "date": parse_date(date),
                }

                for link in div.find_all("a"):
                    url = link.get("href", "")
                    text = link.text.strip()

                    if "youtube.com/channel" in url:
                        activity["channel_name"] = (
                            text if not text.startswith("http") else ""
                        )
                        activity["channel_url"] = url
                    else:
                        activity["content_title"] = (
                            text if not text.startswith("http") else ""
                        )
                        activity["content_url"] = url

                activities.append(activity)
                break

        else:
            logger.warning(f"No matching activity type found for text: {text}")

    return activities


def youtube_html_extractor():
    """
    Extracts activities from HTML, convert to DataFrame, and upload to s3,
    and save to local file.
    """
    logger.info("!!!!!!!!!!!! youtube_html_extractor.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()

        soup = file_manager.load_file(
            FileDirectory.MANUAL_EXPORT_PATH, Google.RAW_HTML_DATA
        )

        activities = extract_activities(soup)
        normalised_data = pd.DataFrame(activities)

        activity_counts = Counter(normalised_data["activity_type"])
        logger.info(f"Activity types found: {dict(activity_counts)}")
        logger.info(f"{len(normalised_data)} activities extracted from HTML file")

        upload_data_s3.post_data_to_s3(normalised_data, "youtube_html", overwrite=True)
        file_manager.save_file(
            FileDirectory.RAW_DATA_PATH, normalised_data, Google.PARSED_HTML_DATA
        )

    except Exception as e:
        error_message = f"Error parsing YouTube HTML data: {e}"
        logger.error(error_message)
        raise Exception(error_message)


if __name__ == "__main__":
    youtube_html_extractor()
