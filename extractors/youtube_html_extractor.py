from collections import Counter

import pandas as pd

from constants import FileDirectory, Google
from utility import s3_manager
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def extract_activities(soup):
    """
    Extracts activity type, published date, channel title, channel URL,
    content title, and content URL from HTML soup.

    Parameters:
        soup: BeautifulSoup object representing HTML soup.

    Returns:
        list: List of dictionaries representing extracted activities.
    """
    logger.info("Extracting activities from HTML...")

    activities = []

    for div in soup.find_all(
        "div", class_="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"
    ):  # Find all activity divs
        text = div.get_text(strip=True)  # Get text from div
        date = div.contents[-1].strip()  # Get date from last element in div

        for activity_type in Google.ACTIVITY_TYPES:
            # Check text starts with predefined activity types (e.g. "Liked", "Disliked", etc.)
            if text.startswith(activity_type):
                activity = {
                    "activity_type": activity_type,
                    "published_at": date,
                }  # Create activity dictionary with type and date

                for link in div.find_all("a"):  # Find all links in div
                    url = link.get("href", "")  # Get link URL
                    text = link.text.strip()  # Get link text

                    if "youtube.com/channel" in url:  # Check if channel URL
                        activity["channel_title"] = (
                            text if not text.startswith("http") else ""
                        )  # Add channel name if text is not URL
                        activity["channel_url"] = url  # Add channel URL
                    else:
                        activity["content_title"] = (
                            text if not text.startswith("http") else ""
                        )  # Add content title if text is not URL
                        activity["content_url"] = url  # Add content URL

                activities.append(activity)  # Add activity to list
                break

        else:
            logger.warning(f"No matching activity type found for text: {text}")

    return activities


def youtube_html_extractor():
    """
    Main function to extract Youtube HTML Data, convert to DataFrame,
    and upload to s3 and local storage.
    """
    logger.info("!!!!!!!!!!!! youtube_html_extractor.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()

        soup = file_manager.load_file(
            FileDirectory.MANUAL_EXPORT_PATH, Google.DATA_KEY, "html"
        )  # Load HTML file

        activities = extract_activities(soup)  # Extract activities from HTML
        act_df = pd.DataFrame(activities)  # Convert to DataFrame

        activity_counts = Counter(act_df["activity_type"])
        logger.info(f"Activity types found: {dict(activity_counts)}")
        logger.info(f"{len(act_df)} activities extracted from HTML file")

        # Upload data to S3 and local storage
        s3_manager.post_data_to_s3("youtube_activity", act_df, True)
        file_manager.save_file(FileDirectory.RAW_DATA_PATH, Google.DATA_KEY, act_df)

    except Exception as e:
        logger.error(f"Error occurred in youtube_html_extractor: {str(e)}")


if __name__ == "__main__":
    youtube_html_extractor()
