from constants import FileDirectory, Strava
from utility import redis_manager
from utility.file_manager import FileManager


def cache():
    file_manager = FileManager()
    strava_data = file_manager.load_file(FileDirectory.RAW_DATA_PATH, Strava.DATA_KEY)

    activity_ids = strava_data["id"]

    redis_manager.update_cached_ids(Strava.ID_KEY, activity_ids)


if __name__ == "__main__":
    cache()
