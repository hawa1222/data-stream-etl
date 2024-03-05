"""
Loads environment variables and sets up configuration settings for the application.

This includes:
- API credentials for Strava and Google.
- Database connection settings.
- Various application settings like timestamp format, timezone, and logging level.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv('/Users/hadid/GitHub/ETL/.env')

# Credentials and tokens for Strava AP
STRAVA_CLIENT_ID = os.environ.get('STRAVA_CLIENT_ID')
STRAVA_CLIENT_SECRET = os.environ.get('STRAVA_CLIENT_SECRET')
STRAVA_ACCESS_TOKEN = os.environ.get('STRAVA_ACCESS_TOKEN')
STRAVA_REFRESH_TOKEN = os.environ.get('STRAVA_REFRESH_TOKEN')

# Credentials and tokens for Google AP
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET')
GOOGLE_ACCESS_TOKEN = os.environ.get('GOOGLE_ACCESS_TOKEN')
GOOGLE_REFRESH_TOKEN = os.environ.get('GOOGLE_REFRESH_TOKEN')
GOOGLE_TOKEN_EXPIRY = os.environ.get('GOOGLE_TOKEN_EXPIRY')

# Database settings
DB_HOST = os.environ.get('MYSQL_HOST', 'localhost')
DB_PORT = os.environ.get('MYSQL_PORT', 3306)
DB_USER = os.environ.get('MYSQL_USER', 'root')
DB_PASSWORD = os.environ.get('MYSQL_PASSWORD')
DB_NAME = os.environ.get('MYSQL_DATABASE')

class Settings:
    
    TIMESTAMP = "%y-%m-%dT%H"
    TIMEZONE = 'Europe/London'
    DATE_FORMAT = '%Y-%m-%d'
    # Logging level
    LOGGING_LEVEL = 'INFO'