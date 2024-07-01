"""
Constants required for Project
"""

import os
import textwrap

#############################################################################################


class FileDirectory:
    """
    Constants related to file directories.
    """

    # Project Folder
    ROOT_DIRECTORY = os.path.abspath(os.path.join(os.path.dirname(__file__), "."))
    # ROOT_DIRECTORY = os.getcwd()
    ENV_PATH = os.path.join(ROOT_DIRECTORY, ".env_dev")

    # Manual Export Folder
    MANUAL_EXPORT_PATH = (
        "/Users/hadid/Library/Mobile Documents/com~apple~CloudDocs/Shared/ETL"
    )
    # Raw Data Folder
    RAW_DATA_PATH = os.path.join(ROOT_DIRECTORY, "data", "raw_data")
    # Clean Data Folder
    CLEAN_DATA_PATH = os.path.join(ROOT_DIRECTORY, "data", "clean_data")
    # Documentation Folder
    DOCUMENTATION_PATH = os.path.join(ROOT_DIRECTORY, "documentation/docs")


class AppleHealth:
    """
    Constants specific to the Apple Health.
    """

    # File Names
    APPLE_XML_PATH = os.path.join(
        FileDirectory.MANUAL_EXPORT_PATH, "apple_health_export"
    )

    APPLE_XML_DATA = "export.xml"
    RECORD_DATA = "apple_record.csv"
    ACTIVITY_DATA = "apple_activity_data.xlsx"

    DOCUMEMTATION_DATA = "apple_data_documentation.xlsx"

    RECORD = "Record"
    ACTIVITY_ELEMENT = "ActivitySummary"
    RECORD_ELEMENTS = [
        "AppleWalkingSteadiness",
        "BasalEnergyBurned",
        "BloodGlucose",
        "FlightsClimbed",
        "HeartRate",
        "HeartRateRecoveryOneMinute",
        "HeartRateVariabilitySDNN",
        "LowHeartRateEvent",
        "MindfulSession",
        "OxygenSaturation",
        "RespiratoryRate",
        "RestingHeartRate",
        "RunningGroundContactTime",
        "RunningPower",
        "RunningSpeed",
        "RunningStrideLength",
        "RunningVerticalOscillation",
        "SleepAnalysis",
        "StepCount",
        "VO2Max",
        "WalkingAsymmetryPercentage",
        "WalkingDoubleSupportPercentage",
        "WalkingHeartRateAverage",
        "WalkingSpeed",
        "WalkingStepLength",
    ]

    # Fields
    DATE_COMPONENTS = "date_components"
    ACTIVE_ENERGY_BURNED = "active_energy_burned"
    APPLE_EXERCISE_TIME = "apple_exercise_time"
    APPLE_STAND_HOURS = "apple_stand_hours"
    CREATION_DATE = "creation_date"
    START_DATE = "start_date"
    END_DATE = "end_date"
    DATE = "date"
    HOUR = "hour"
    ORIGINAL_VALUE = "original_value"
    VALUE = "value"
    TYPE_FIELD = "type"
    SOURCE_NAME = "source_name"
    PRIORITY = "priority"
    DURATION = "duration"
    BED_TIME = "bed_time"
    AWAKE_TIME = "awake_time"
    TIME_IN_BED = "time_in_bed"

    # Data Source Priorities
    PRIORITY_DICT = {
        "HW3 iWatch": 1,
        "HW3 iPhone": 2,
        "Clock": 3,
        "Sleep Cycle": 4,
        "Health": 5,
        "Dexcom G7": 6,
        "Headspace": 7,
        "RunGap": 8,
        "Blood Oxygen": 9,
    }

    # Transformation Logic
    RECORD_TRANSFORMATION_LOGIC = {
        "AppleWalkingSteadiness": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["walking_steadiness_pct"],
            "group": ["walking_metrics"],
        },
        "BasalEnergyBurned": {
            "timeframe": ["date"],
            "agg_type": ["Sum"],
            "fields": ["basal_energy_kcal"],
            "group": ["daily_activity"],
        },
        "BloodGlucose": {
            "timeframe": ["date", "hour"],
            "agg_type": ["Mean"],
            "fields": ["avg_blood_glucose_mmol"],
            "group": ["blood_glucose"],
        },
        "FlightsClimbed": {
            "timeframe": ["date"],
            "agg_type": ["Sum"],
            "fields": ["flight_climbed"],
            "group": ["daily_activity"],
        },
        "HeartRate": {
            "timeframe": ["date", "hour"],
            "agg_type": ["Mean"],
            "fields": ["avg_HR_rate"],
            "group": ["heart_rate"],
        },
        "HeartRateRecoveryOneMinute": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["avg_1min_HR_recovery"],
            "group": ["fitness_metrics"],
        },
        "HeartRateVariabilitySDNN": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["avg_HR_variability"],
            "group": ["fitness_metrics"],
        },
        "LowHeartRateEvent": {
            "timeframe": ["date", "hour"],
            "agg_type": ["Sum"],
            "fields": ["low_HR_event"],
            "group": ["low_HR_events"],
        },
        "MindfulSession": {
            "timeframe": ["date"],
            "agg_type": ["Sum"],
            "fields": ["mindful_duration", "mindful_count"],
            "group": ["daily_activity"],
        },
        "OxygenSaturation": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["avg_oxg_saturation"],
            "group": ["fitness_metrics"],
        },
        "RespiratoryRate": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["avg_respiratory_pm"],
            "group": ["fitness_metrics"],
        },
        "RestingHeartRate": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["avg_resting_HR"],
            "group": ["fitness_metrics"],
        },
        "RunningGroundContactTime": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["avg_run_gct_ms"],
            "group": ["running_metrics"],
        },
        "RunningPower": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["avg_run_pwr_w"],
            "group": ["running_metrics"],
        },
        "RunningSpeed": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["avg_run_spd_kmh"],
            "group": ["running_metrics"],
        },
        "RunningStrideLength": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["run_stride_len_m"],
            "group": ["running_metrics"],
        },
        "RunningVerticalOscillation": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["run_vert_osc_cm"],
            "group": ["running_metrics"],
        },
        "SleepAnalysis": {
            "timeframe": ["date"],
            "agg_type": ["Sum"],
            "fields": [
                "total_time_asleep",
                "bed_time",
                "awake_time",
                "sleep_counts",
                "rem_cycles",
            ],
            "group": ["sleep"],
        },
        "StepCount": {
            "timeframe": ["date", "hour"],
            "agg_type": ["Sum"],
            "fields": ["step_count"],
            "group": ["steps"],
        },
        "VO2Max": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["vo2Max_mLmin_kg"],
            "group": ["fitness_metrics"],
        },
        "WalkingAsymmetryPercentage": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["walking_asymm_pct"],
            "group": ["walking_metrics"],
        },
        "WalkingDoubleSupportPercentage": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["walking_ds_pct"],
            "group": ["walking_metrics"],
        },
        "WalkingHeartRateAverage": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["walking_avg_HR"],
            "group": ["walking_metrics"],
        },
        "WalkingSpeed": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["walking_speed_kmhr"],
            "group": ["walking_metrics"],
        },
        "WalkingStepLength": {
            "timeframe": ["date"],
            "agg_type": ["Mean"],
            "fields": ["walking_step_len_cm"],
            "group": ["walking_metrics"],
        },
    }

    ACTIVITY_TRANSFORMATION_LOGIC = {
        "ActivitySummary": {
            "timeframe": ["date"],
            "agg_type": ["Sum"],
            "fields": [
                "date_components",
                "active_energy_burned",
                "apple_exercise_time",
                "apple_stand_hours",
            ],
            "group": ["daily_activity"],
        }
    }

    # Define logic documentation (details can be filled in later)
    SCRIPT_LOGIC = textwrap.dedent("""\
        Apple Data Documentation
        1
        2
        3
        4
        5
        """)


class Daylio:
    """
    Constants specific to the Daylio data.
    """

    # File Names
    RAW_DATA = "daylio_export.csv"

    CLEAN_DATA = "daylio_data.xlsx"

    MOOD_DATA = "daylio_mood.xlsx"
    ACTIVITY_DATA = "daylio_activities.xlsx"

    DOCUMEMTATION_DATA = "daylio_data_documentation.xlsx"

    # Fields
    ID = "id"
    DATE = "full_date"
    TIME = "time"
    DATE_TIME = "date_time"
    ACTIVITY = "activities"

    CLEAN_FIELDS = ["date_time", "mood", "note_title", "note", ACTIVITY]

    # Documenter
    SCRIPT_LOGIC = textwrap.dedent("""\
        Daylio Data Documentation
        1
        2
        3
        4
        5
        """)


class Spend:
    """
    Constants specific to the Daylio data.
    """

    # File Names
    RAW_DATA = "Budget.xlsm"
    RAW_SHEET_NAME = "Spending Data"

    CLEAN_DATA = "spend_transactions.xlsx"

    DOCUMEMTATION_DATA = "spend_documentation.xlsx"

    # Fields
    DATE = "date"

    # Documenter
    SCRIPT_LOGIC = textwrap.dedent("""\
        Spend Data Documentation
        1
        2
        3
        4
        5
        """)


class APIHandler:
    HTTP_200_OK = 200
    HTTP_400_BAD_REQUEST = 400
    HTTP_401_UNAUTHORISED = 401
    HTTP_404_NOT_FOUND = 404
    HTTP_429_RATE_LIMITED = 429
    HTTP_500_SERVER_ERROR = 500

    RATE_LIMIT_SLEEP_TIME = 15 * 60
    ITEMS_PER_PAGE = 50


class StravaAPI:
    """
    Constants specific to the Strava API.
    """

    # File Names
    COMPLETE_DATA = "strava_data.xlsx"

    ACTIVITY_DATA = "strava_activity.xlsx"
    PERFORMANCE_DATA = "strava_performance_metrics.xlsx"

    DOCUMEMTATION_DATA = "strava_data_documentation.xlsx"

    # API Parameters
    BASE_URL = "https://www.strava.com/api/v3/athlete/activities"
    ACTIVITY_URL = "https://www.strava.com/api/v3/activities/{}"
    TOKEN_URL = "https://www.strava.com/oauth/token"

    AUTH_HEADER = {"Authorization": None}

    HTTP_200_OK = 200
    HTTP_400_BAD_REQUEST = 400
    HTTP_401_UNAUTHORIZED = 401
    HTTP_429_RATE_LIMITED = 429

    RATE_LIMIT_SLEEP_TIME = 15 * 60  # Time in seconds
    ITEMS_PER_PAGE = 50

    # Fields
    LEGACY_ACT_ID = "id"
    ACTIVITY_ID = "activity_id"

    LEGACY_ACT_NAME = "name"
    ACTIVITY_NAME = "activity_name"

    DATE = "start_date"
    SPORT = "sport_type"
    MOVE_TIME = "moving_time"
    ELAP_TIME = "elapsed_time"
    LEGACY_GEAR = "name_2"
    GEAR_NAME = "gear_name"

    SPORT_TEXT = "Workout"
    SPORT_TEXT_NEW = "Martial Arts"

    CLEAN_FIELDS = [
        ACTIVITY_ID,
        "external_id",
        "device_name",
        GEAR_NAME,
        ACTIVITY_NAME,
        SPORT,
        DATE,
        "distance",
        MOVE_TIME,
        ELAP_TIME,
        "average_speed",
        "max_speed",
        "elev_high",
        "elev_low",
        "average_cadence",
        "average_heartrate",
        "max_heartrate",
        "calories",
        "suffer_score",
        "private_note",
        "polyline",
    ]

    ACTIVITY_FIELDS = [
        ACTIVITY_ID,
        "external_id",
        "device_name",
        ACTIVITY_NAME,
        SPORT,
        "start_date",
        GEAR_NAME,
        "private_note",
        "polyline",
    ]

    PERFORMANCE_FIELDS = [
        ACTIVITY_ID,
        "distance",
        MOVE_TIME,
        ELAP_TIME,
        "average_speed",
        "max_speed",
        "average_cadence",
        "average_heartrate",
        "max_heartrate",
        "calories",
        "suffer_score",
    ]

    # Documenter
    SCRIPT_LOGIC = textwrap.dedent("""\
        Strava Data Documentation
        1
        2
        3
        4
        5
        """)


class Google:
    """
    Constants specific to Google API.
    """

    # File Names
    RAW_HTML_DATA = "MyActivity.html"
    PARSED_HTML_DATA = "youtube_activity.xlsx"

    TOKEN_FILE = os.path.join(
        FileDirectory.ROOT_DIRECTORY, "credentials/google/token.json"
    )
    CLEAN_PLAYLIST_DATA = "youtube_likes_dislikes.xlsx"
    CHANNEL_DATA = "youtube_channel.xlsx"
    CACHE_LIKES_DATA = "youtube_likes.xlsx"
    SUBS_DATA = "youtube_subscriptions.xlsx"

    DOCUMENTATION_DATA = "youtube_data_documentation.xlsx"

    # API Parameters
    TOKEN_URL = "https://oauth2.googleapis.com/token"

    SCOPES = [
        "https://www.googleapis.com/auth/youtube.readonly",
        "https://www.googleapis.com/auth/youtube",
    ]

    CHANNEL_API_CALL = "channels"
    PLAYLIST_API_CALL = "playlistItems"
    SUBS_API_CALL = "subscriptions"
    MAX_RESULTS = 50
    API_CONFIG = {
        "channels": {
            "api_endpoint": "channels",
            "parameters": {
                "id": "UCjCBfi9gMp0tFlaDzDiu_iQ",
                # "mine": True,
                "part": "snippet",
                "maxResults": MAX_RESULTS,
            },
        },
        "subs": {
            "api_endpoint": "subscriptions",
            "parameters": {"mine": True, "part": "snippet", "maxResults": MAX_RESULTS},
        },
        "likes": {
            "api_endpoint": "playlistItems",
            "parameters": {
                "playlistId": "LLjCBfi9gMp0tFlaDzDiu_iQ",
                "part": "snippet",
                "maxResults": MAX_RESULTS,
            },
        },
    }

    # Fields
    DESC = "description"
    DATE = "published_at"
    THUMBNAIL = "thumbnail_url"
    PLAYLIST = "playlist"

    ACTIVITY_TYPES = ["Liked", "Disliked", "Subscribed", "Voted", "Saved"]

    SOURCE = "source"
    SOURCE_VALUE = ["API", "HTML"]
    LEGACY_VID_ID = "resource_id_video_id"
    LEGACY_CHANNEL_ID = "resource_id_channel_id"
    VID_TITLE = "title"
    VID_ID = "video_id"
    CHANNEL_ID = "channel_id"
    CHANNEL_URL = "channel_url"
    VID_URL = "video_url"
    VID_OWNER = "video_owner_channel_title"

    CHANNEL_FIELDS = [DATE, "id", "title", "custom_url", THUMBNAIL, DESC]
    LIKES_FIELDS = [
        DATE,
        "playlist",
        "source",
        "video_id",
        "title",
        DESC,
        THUMBNAIL,
        "video_owner_channel_title",
        "channel_id",
    ]
    SUBS_FIELDS = [DATE, "resource_id_channel_id", "title", DESC, THUMBNAIL]

    # Documenter
    SCRIPT_LOGIC = textwrap.dedent("""\
        Youtube Data Documentation
        1
        2
        3
        4
        5
        """)
