from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from constants import FileDirectory
from utility.database_manager import DatabaseHandler
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def apple_loader():
    """
    Main function to load Apple data, create tables, insert data into database.
    """
    logger.info("!!!!!!!!!!!! apple_loader.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()
        db_handler = DatabaseHandler(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

        field_structures = {
            "apple_walking_metrics": {
                "date": "DATE NOT NULL PRIMARY KEY",
                "walking_steadiness_pct": "DECIMAL(5,2)",
                "walking_asymm_pct": "DECIMAL(5,2)",
                "walking_ds_pct": "DECIMAL(5,2)",
                "walking_avg_HR": "DECIMAL(5,2)",
                "walking_speed_kmhr": "DECIMAL(5,2)",
                "walking_step_len_cm": "DECIMAL(5,2)",
            },
            "apple_daily_activity": {
                "date": "DATE NOT NULL PRIMARY KEY",
                "basal_energy_kcal": "DECIMAL(6,2)",
                "flight_climbed": "SMALLINT UNSIGNED",
                "mindful_duration": "DECIMAL(5,2)",
                "mindful_count": "TINYINT UNSIGNED",
                "active_energy_burned": "DECIMAL(6,2)",
                "apple_exercise_time": "SMALLINT UNSIGNED",
                "apple_stand_hours": "TINYINT UNSIGNED",
            },
            "apple_steps": {
                "date": "DATE NOT NULL",
                "hour": "TINYINT UNSIGNED NOT NULL",
                "step_count": "SMALLINT UNSIGNED",
                "PRIMARY KEY": "(date, hour)",
            },
            "apple_running_metrics": {
                "date": "DATE NOT NULL PRIMARY KEY",
                "avg_run_gct_ms": "DECIMAL(5,2)",
                "avg_run_pwr_w": "DECIMAL(5,2)",
                "avg_run_spd_kmh": "DECIMAL(4,2)",
                "run_stride_len_m": "DECIMAL(4,2)",
                "run_vert_osc_cm": "DECIMAL(4,2)",
            },
            "apple_sleep": {
                "date": "DATE NOT NULL PRIMARY KEY",
                "asleep_core": "DECIMAL(4,2)",
                "asleep_deep": "DECIMAL(4,2)",
                "asleep_rem": "DECIMAL(4,2)",
                "asleep_unspecified": "DECIMAL(4,2)",
                "awake": "DECIMAL(4,2)",
                "total_sleep": "DECIMAL(4,2)",
                "bed_time": "TIMESTAMP",
                "awake_time": "TIMESTAMP",
                "source_name": "VARCHAR(50)",
                "time_in_bed": "DECIMAL(4,2)",
            },
            "apple_fitness_metrics": {
                "date": "DATE NOT NULL PRIMARY KEY",
                "avg_1min_HR_recovery": "DECIMAL(5,2)",
                "avg_HR_variability": "DECIMAL(5,2)",
                "avg_oxg_saturation": "DECIMAL(5,2)",
                "avg_respiratory_pm": "DECIMAL(5,2)",
                "avg_resting_HR": "DECIMAL(5,2)",
                "vo2Max_mLmin_kg": "DECIMAL(5,2)",
            },
            "apple_low_hr_events": {
                "date": "DATE NOT NULL",
                "hour": "TINYINT UNSIGNED NOT NULL",
                "low_HR_event": "TINYINT UNSIGNED",
                "PRIMARY KEY": "(date, hour)",
            },
            "apple_blood_glucose": {
                "date": "DATE NOT NULL",
                "hour": "TINYINT UNSIGNED NOT NULL",
                "avg_blood_glucose_mmol": "DECIMAL(5,2)",
                "PRIMARY KEY": "(date, hour)",
            },
            "apple_heart_rate": {
                "date": "DATE NOT NULL",
                "hour": "TINYINT UNSIGNED NOT NULL",
                "avg_HR_rate": "DECIMAL(5,2)",
                "PRIMARY KEY": "(date, hour)",
            },
        }

        dataframes = {
            name: file_manager.load_file(FileDirectory.CLEAN_DATA_PATH, name)
            for name in field_structures.keys()
        }

        for name, fields in field_structures.items():
            db_handler.create_table(name, fields)

            column_names = [
                field for field in fields if not field.startswith("PRIMARY KEY")
            ]

            db_handler.insert_data(name, dataframes[name], column_names)

        db_handler.close_connection()

    except Exception as e:
        logger.error(f"Error occurred in apple_loader: {str(e)}")


if __name__ == "__main__":
    apple_loader()
