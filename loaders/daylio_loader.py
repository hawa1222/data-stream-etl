from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from constants import Daylio, FileDirectory
from utility.database_manager import DatabaseHandler
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def daylio_loader():
    """
    Main function to load Daylio data, create tables, insert data into database.
    """
    logger.info("!!!!!!!!!!!! daylio_loader.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()
        db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

        datasets = {
            Daylio.MOOD_DATA: {
                "date_time": "TIMESTAMP NOT NULL PRIMARY KEY",
                "mood_score": "TINYINT UNSIGNED NOT NULL",
                "mood_category": "VARCHAR(20) NOT NULL",
                "note_title": "TEXT",
                "note": "TEXT",
            },
            Daylio.ACTIVITY_DATA: {
                "id": "SMALLINT UNSIGNED NOT NULL PRIMARY KEY",
                "date_time": f"TIMESTAMP NOT NULL, FOREIGN KEY (date_time) REFERENCES {Daylio.MOOD_DATA}(date_time)",
                "activities": "VARCHAR(50)",
            },
        }

        for data_name, fields in datasets.items():
            df = file_manager.load_file(FileDirectory.CLEAN_DATA_PATH, data_name)
            db_handler.create_table(data_name, fields)
            db_handler.insert_data(data_name, df, list(fields.keys()))

        db_handler.close_connection()

    except Exception as e:
        logger.error(f"Error occurred in daylio_loader: {str(e)}")


if __name__ == "__main__":
    daylio_loader()
