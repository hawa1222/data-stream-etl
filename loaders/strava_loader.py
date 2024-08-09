from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from constants import FileDirectory, Strava
from utility.database_manager import DatabaseHandler
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def strava_loader():
    """
    Main function to load Strava data, create tables, insert data into database.
    """
    logger.info("!!!!!!!!!!!! strava_loader.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()
        db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

        datasets = {
            Strava.ACTIVITY_DATA: {
                "activity_id": "BIGINT UNSIGNED NOT NULL PRIMARY KEY",
                "external_id": "VARCHAR(100)",
                "device_name": "VARCHAR(50)",
                "activity_name": "VARCHAR(50)",
                "sport_type": "VARCHAR(50)",
                "start_date": "TIMESTAMP",
                "gear_name": "VARCHAR(50)",
                "private_note": "VARCHAR(100)",
                "map_polyline": "TEXT",
            },
            Strava.PERFORMANCE_DATA: {
                "activity_id": f"BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
                f"FOREIGN KEY (activity_id) REFERENCES {Strava.ACTIVITY_DATA}(activity_id)",
                "distance": "DECIMAL(6,1)",
                "moving_time": "DECIMAL(5,2)",
                "elapsed_time": "DECIMAL(5,2)",
                "average_speed": "DECIMAL(4,2)",
                "max_speed": "DECIMAL(5,2)",
                "average_cadence": "DECIMAL(4,1)",
                "average_heartrate": "DECIMAL(4,1)",
                "max_heartrate": "TINYINT UNSIGNED",
                "calories": "DECIMAL(5,1)",
                "suffer_score": "TINYINT UNSIGNED",
            },
        }

        for data_name, fields in datasets.items():
            df = file_manager.load_file(FileDirectory.CLEAN_DATA_PATH, data_name)
            db_handler.create_table(data_name, fields)
            db_handler.insert_data(data_name, df, list(fields.keys()))

        db_handler.close_connection()

    except Exception as e:
        logger.error(f"Error occurred in strava_loader: {str(e)}")


if __name__ == "__main__":
    strava_loader()
