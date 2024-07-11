from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from constants import FileDirectory, Google
from utility.database_manager import DatabaseHandler
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def youtube_loader():
    """
    Main function to load YouTube data, create tables, insert data into database.
    """
    logger.info("!!!!!!!!!!!! youtube_loader.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()
        db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

        ACTIVITY_DATA = Google.DATA_KEY + "_enriched"

        datasets = {
            ACTIVITY_DATA: {
                "key_id": "VARCHAR(70) NOT NULL PRIMARY KEY",
                "source": "VARCHAR(10)",
                "activity_type": "VARCHAR(12)",
                "published_at": "TIMESTAMP NOT NULL",
                "content_id": "VARCHAR(20)",
                "content_title": "TEXT",
                "content_desc": "TEXT",
                "content_url": "VARCHAR(255)",
                "content_thumbnail": "VARCHAR(255)",
                "channel_id": "VARCHAR(30)",
                "channel_title": "VARCHAR(60)",
                "channel_desc": "TEXT",
                "channel_url": "VARCHAR(255)",
                "channel_thumbnail": "VARCHAR(255)",
            }
        }

        for data_name, fields in datasets.items():
            df = file_manager.load_file(FileDirectory.CLEAN_DATA_PATH, data_name)
            db_handler.create_table(data_name, fields)
            db_handler.insert_data(data_name, df, list(fields.keys()))

        db_handler.close_connection()

    except Exception as e:
        logger.error(f"Error occurred in youtube_loader: {str(e)}")


if __name__ == "__main__":
    youtube_loader()
