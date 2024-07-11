"""
Constants required for Project
"""

import os


class FileDirectory:
    """
    Constants related to file directories.
    """

    ROOT_DIRECTORY = os.path.abspath(os.path.join(os.path.dirname(__file__), "."))
    ENV_PATH = os.path.join(ROOT_DIRECTORY, ".env_dev")
    MANUAL_EXPORT_PATH = (
        "/Users/hadid/Library/Mobile Documents/com~apple~CloudDocs/Shared/ETL"
    )
    RAW_DATA_PATH = os.path.join(ROOT_DIRECTORY, "data", "raw_data")
    CLEAN_DATA_PATH = os.path.join(ROOT_DIRECTORY, "data", "clean_data")
    DOCUMENTATION_PATH = os.path.join(ROOT_DIRECTORY, "documentation/docs")


class Daylio:
    """
    Constants specific to the Daylio data.
    """

    DATA_KEY = "daylio_journal"
    MOOD_DATA = "daylio_mood"
    ACTIVITY_DATA = "daylio_activities"

    ID = "id"
    DATE = "full_date"
    TIME = "time"
    DATE_TIME = "date_time"
    ACTIVITY = "activities"

    CLEAN_FIELDS = [DATE_TIME, "mood", "note_title", "note", ACTIVITY]


class Spend:
    """
    Constants specific to the Spend data.
    """

    DATA_KEY = "transactions"
    RAW_SHEET_NAME = "Spending Data"
    DATE = "date"


class Apple:
    """
    Constants specific to the Apple Health.
    """

    XML_DATA = "apple_health"
    RECORD_DATA = "apple_records"
    ACTIVITY_DATA = "apple_activity"

    ACTIVITY_ELEMENT = "ActivitySummary"
    DATE_COMPONENTS = "date_components"
    ACTIVE_ENERGY_BURNED = "active_energy_burned"
    APPLE_EXERCISE_TIME = "apple_exercise_time"
    APPLE_STAND_HOURS = "apple_stand_hours"

    RECORD_ELEMENT = "Record"
    RECORD_TYPE = [
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


class Google:
    """
    Constants specific to the Google API.
    """

    TOKEN_FILE = os.path.join(
        FileDirectory.ROOT_DIRECTORY, "credentials/google/token.json"
    )

    DATA_KEY = "youtube_activity"
    LIKES_DATA = "youtube_likes"
    SUBS_DATA = "youtube_subs"
    CHANNEL_DATA = "youtube_channels"

    TOKEN_URL = "https://oauth2.googleapis.com/token"
    SCOPES = [
        "https://www.googleapis.com/auth/youtube.readonly",
        "https://www.googleapis.com/auth/youtube",
    ]

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

    # Legacy Fields
    LEGACY_DATE = "snippet.published_at"
    LEGACY_VID_ID = "snippet.resource_id.video_id"
    LEGACY_TITLE = "snippet.title"
    LEGACY_DESC = "snippet.description"

    LEGACY_CHANNEL_ID = "snippet.video_owner_channel_id"
    LEGACY_SUB_CHANNEL_ID = "snippet.resource_id.channel_id"
    LEGACY_CHANNEL_TITLE = "snippet.video_owner_channel_title"

    # New Fields
    ACTIVITY_TYPE = "activity_type"
    ACTIVITY_TYPES = ["Liked", "Disliked", "Subscribed", "Voted", "Saved"]
    SOURCE = "source"
    SOURCE_VALUE = ["API", "HTML"]
    DATE = "published_at"

    CONTENT_ID = "content_id"
    CONTENT_TITLE = "content_title"
    CONTENT_DESC = "content_desc"
    CONTENT_URL = "content_url"
    CONTENT_THUMBNAIL = "content_thumbnail"

    CHANNEL_ID = "channel_id"
    CHANNEL_TITLE = "channel_title"
    CHANNEL_DESC = "channel_desc"
    CHANNEL_URL = "channel_url"
    CHANNEL_THUMBNAIL = "channel_thumbnail"

    LIKES_MAPPING = {
        SOURCE: SOURCE,
        ACTIVITY_TYPE: ACTIVITY_TYPE,
        LEGACY_DATE: DATE,
        LEGACY_VID_ID: CONTENT_ID,
        LEGACY_TITLE: CONTENT_TITLE,
        LEGACY_DESC: CONTENT_DESC,
        CONTENT_URL: CONTENT_URL,
        CONTENT_THUMBNAIL: CONTENT_THUMBNAIL,
        LEGACY_CHANNEL_ID: CHANNEL_ID,
        LEGACY_CHANNEL_TITLE: CHANNEL_TITLE,
        CHANNEL_URL: CHANNEL_URL,
    }

    SUBS_MAPPING = {
        SOURCE: SOURCE,
        ACTIVITY_TYPE: ACTIVITY_TYPE,
        LEGACY_DATE: DATE,
        LEGACY_SUB_CHANNEL_ID: CHANNEL_ID,
        LEGACY_TITLE: CHANNEL_TITLE,
        LEGACY_DESC: CHANNEL_DESC,
        CHANNEL_URL: CHANNEL_URL,
        CHANNEL_THUMBNAIL: CHANNEL_THUMBNAIL,
    }

    ACTIVITY_FIELDS = [
        SOURCE,
        ACTIVITY_TYPE,
        DATE,
        CONTENT_ID,
        CONTENT_TITLE,
        CONTENT_DESC,
        CONTENT_URL,
        CONTENT_THUMBNAIL,
        CHANNEL_ID,
        CHANNEL_TITLE,
        CHANNEL_DESC,
        CHANNEL_URL,
        CHANNEL_THUMBNAIL,
    ]


class Strava:
    """
    Constants specific to the Strava API.
    """

    # File Names
    ID_KEY = "strava_activity_ids"
    DATA_KEY = "strava_activity"

    ACTIVITY_DATA = "strava_events"
    PERFORMANCE_DATA = "strava_metrics"

    # API Parameters
    BASE_URL = "https://www.strava.com/api/v3/athlete/activities"
    ACTIVITY_URL = "https://www.strava.com/api/v3/activities/{}"
    TOKEN_URL = "https://www.strava.com/oauth/token"

    # Fields
    LEGACY_ACT_ID = "id"
    ACTIVITY_ID = "activity_id"

    LEGACY_ACT_NAME = "name"
    ACTIVITY_NAME = "activity_name"

    DATE = "start_date"
    SPORT = "sport_type"
    MOVE_TIME = "moving_time"
    ELAP_TIME = "elapsed_time"

    LEGACY_GEAR = "gear.name"
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
        "map_polyline",
    ]

    ACTIVITY_FIELDS = [
        ACTIVITY_ID,
        "external_id",
        "device_name",
        ACTIVITY_NAME,
        SPORT,
        DATE,
        GEAR_NAME,
        "private_note",
        "map_polyline",
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


class APIHandler:
    """
    Constants related to API handling.
    """

    HTTP_200_OK = 200
    HTTP_400_BAD_REQUEST = 400
    HTTP_401_UNAUTHORISED = 401
    HTTP_404_NOT_FOUND = 404
    HTTP_429_RATE_LIMITED = 429
    HTTP_500_SERVER_ERROR = 500

    RATE_LIMIT_SLEEP_TIME = 15 * 60
    ITEMS_PER_PAGE = 50
