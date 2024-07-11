from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from constants import FileDirectory, Spend
from utility.database_manager import DatabaseHandler
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def spend_loader():
    """
    Main function to load Spend data, create tables, insert data into database.
    """
    logger.info("!!!!!!!!!!!! spend_loader.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()
        db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

        schema = {
            "transaction_id": "INT UNSIGNED NOT NULL PRIMARY KEY",
            "category_a": "VARCHAR(50) NOT NULL",
            "category_b": "VARCHAR(50) NOT NULL",
            "outlet": "VARCHAR(50) NOT NULL",
            "description": "TEXT",
            "amount": "DECIMAL(7,2)",
            "date": "DATE NOT NULL",
            "period": "VARCHAR(8)",
        }

        df = file_manager.load_file(FileDirectory.CLEAN_DATA_PATH, Spend.DATA_KEY)
        db_handler.create_table(Spend.DATA_KEY, schema)
        db_handler.insert_data(Spend.DATA_KEY, df, list(schema.keys()))

        db_handler.close_connection()

    except Exception as e:
        logger.error(f"Error occurred in spend_loader: {str(e)}")


if __name__ == "__main__":
    spend_loader()
