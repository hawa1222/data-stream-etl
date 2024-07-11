from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from constants import Daylio, Google, Spend, Strava
from utility.database_manager import DatabaseHandler
from utility.log_manager import setup_logging

logger = setup_logging()


def delete_data():
    """
    Main function to delete all data from the database.
    """
    logger.info("!!!!!!!!!!!! delete_data.py !!!!!!!!!!!")

    try:
        YT = f"{Google.DATA_KEY}_enriched"
        table_names = {
            "apple_walking_metrics",
            "apple_daily_activity",
            "apple_blood_glucose",
            "apple_heart_rate",
            "apple_fitness_metrics",
            "apple_low_hr_events",
            "apple_running_metrics",
            "apple_sleep",
            "apple_steps",
            Daylio.ACTIVITY_DATA,
            Daylio.MOOD_DATA,
            Spend.DATA_KEY,
            Strava.PERFORMANCE_DATA,
            Strava.ACTIVITY_DATA,
            YT,
        }

        db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

        for table in table_names:
            db_handler.drop_table(table)

        db_handler.close_connection()

        logger.info(
            f"Successfully deleted {len(table_names)} tables from the database."
        )

    except Exception as e:
        logger.error(f"Error occurred in delete_data: {str(e)}")


if __name__ == "__main__":
    delete_data()
