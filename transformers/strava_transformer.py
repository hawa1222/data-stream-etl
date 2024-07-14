from constants import FileDirectory, Strava
from utility.clean_data import round_floats
from utility.clean_dates import parse_date
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def clean_data(df):
    """
    Cleans data by renaming columns, selecting specified columns, replacing values,
    and converting time from seconds to minutes.

    Parameters:
        df: Input DataFrame containing raw data.

    Returns:
        DataFrame: Cleaned DataFrame.
    """

    try:
        logger.info("Cleaning data...")

        logger.info(f"Stardardising {Strava.DATE} column...")
        logger.info(f"Sample before cleaning: {df[Strava.DATE].head(2).to_list()}")
        df[Strava.DATE] = df[Strava.DATE].apply(parse_date)
        logger.info(f"Sample after cleaning: {df[Strava.DATE].head(2).to_list()}")

        fields_mapping = {
            Strava.LEGACY_GEAR: Strava.GEAR_NAME,
            Strava.LEGACY_ACT_ID: Strava.ACTIVITY_ID,
            Strava.LEGACY_ACT_NAME: Strava.ACTIVITY_NAME,
            "map.polyline": "map_polyline",
        }
        df = df.rename(columns=fields_mapping)
        logger.info(
            f"Renamed columns '{', '.join(fields_mapping.keys())}' to '{', '.join(fields_mapping.values())}'"
        )

        df = df[Strava.CLEAN_FIELDS]

        df[Strava.SPORT] = df[Strava.SPORT].replace(
            Strava.SPORT_TEXT, Strava.SPORT_TEXT_NEW
        )  # Replace sport text

        df[Strava.SPORT] = df[Strava.SPORT].str.replace(
            r"(?<=[a-z])(?=[A-Z])", " ", regex=True
        )  # Add space between lowercase and uppercase letters

        # Convert times from seconds to minutes
        df[Strava.MOVE_TIME] = df[Strava.MOVE_TIME] / 60
        df[Strava.ELAP_TIME] = df[Strava.ELAP_TIME] / 60

        df = round_floats(df)  # Round float fields to 2 DP

        logger.info("Successfully cleaned data")

        return df

    except Exception as e:
        logger.error(f"Error occurred in transform_data: {str(e)}")
        raise


def strava_transformer():
    """
    Main function to load Strava Data, clean & transform, split into two DataFrames
    based on specified columns, and save to local storage.
    """
    logger.info("!!!!!!!!!!!! strava_transformer.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()

        strava_data = file_manager.load_file(
            FileDirectory.RAW_DATA_PATH, Strava.DATA_KEY
        )

        clean_strava_data = clean_data(strava_data)

        # Split data into two DataFrames based on specified columns
        df_activity = clean_strava_data[Strava.ACTIVITY_FIELDS]
        df_performance_metrics = clean_strava_data[Strava.PERFORMANCE_FIELDS]
        logger.info(
            "Partially normalised data into two tables: df_activity, df_performance_metrics"
        )

        datasets = {
            Strava.DATA_KEY: clean_strava_data,
            Strava.ACTIVITY_DATA: df_activity,
            Strava.PERFORMANCE_DATA: df_performance_metrics,
        }

        for file_name, dataframe in datasets.items():
            file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, file_name, dataframe)

    except Exception as e:
        logger.error(f"Error occurred in strava_transformer: {str(e)}")


if __name__ == "__main__":
    strava_transformer()
