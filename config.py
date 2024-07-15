"""
Script to load environment variables and define application settings.

Includes:
- API credentials for Strava
- Database connection settings.
- Various application settings like timestamp format, timezone, and logging level.
"""

import os

from dotenv import load_dotenv

load_dotenv(".env_dev")

STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")
STRAVA_ACCESS_TOKEN = os.environ.get("STRAVA_ACCESS_TOKEN")
STRAVA_REFRESH_TOKEN = os.environ.get("STRAVA_REFRESH_TOKEN")


DB_HOST = os.environ.get("MYSQL_HOST", "localhost")
DB_PORT = os.environ.get("MYSQL_PORT", 3306)
DB_USER = os.environ.get("MYSQL_USER", "root")
DB_PASSWORD = os.environ.get("MYSQL_PASSWORD")
DB_NAME = os.environ.get("MYSQL_DATABASE")

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", 6379)
REDIS_DB = os.environ.get("REDIS_DB", 0)

S3_BUCKET = "etl-5h3gn2wqhzfd"


class Settings:
    TIMEZONE = "Europe/London"
    TIMESTAMP = "%y-%m-%dT%H"
    DATE_FORMAT = "%Y-%m-%d"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    LOGGING_LEVEL = "DEBUG"

    RATE_LIMIT_SLEEP_TIME = 15 * 60
    ITEMS_PER_PAGE = 50
