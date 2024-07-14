import pandas as pd

from config import Settings
from constants import Apple, FileDirectory
from utility.clean_data import CleanData, round_floats
from utility.clean_dates import parse_date
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def process_activity(df):
    """
    Transforms and filters 'ActivitySummary' DataFrame.

    Parameters:
        df: DataFrame to transform.

    Returns:
        pd.DataFrame: Transformed DataFrame.
    """
    try:
        logger.info("Processing 'ActivitySummary' DataFrame...")

        df_copy = df.copy()

        fields = [
            Apple.DATE_COMPONENTS,
            Apple.ACTIVE_ENERGY_BURNED,
            Apple.APPLE_EXERCISE_TIME,
            Apple.APPLE_STAND_HOURS,
        ]

        df_copy = df_copy[fields]

        for col in fields[1:]:
            df_copy[col] = pd.to_numeric(df_copy[col], errors="coerce")

        df_copy.rename(columns={fields[0]: Apple.DATE}, inplace=True)

        logger.info("Successfully processed 'ActivitySummary' DataFrame")

        return df_copy

    except Exception as e:
        logger.error(
            f"Error occurred while processing 'ActivitySummary' DataFrame: {str(e)}"
        )
        return pd.DataFrame()


def enrich_record(df):
    """
    Transforms DataFrame with various operations like timesone conversion, data extraction, and so on.

    Parameters:
        df: DataFrame to transform.

    Returns:
        pd.DataFrame: Transformed DataFrame.
    """

    logger.info("Enriching 'Record' DataFrame...")

    try:
        df = df.copy()

        date_cols = [
            Apple.CREATION_DATE,
            Apple.START_DATE,
            Apple.END_DATE,
        ]

        logger.info("Converting date columns to datetime format...")
        logger.info(
            f"Two date example values before transformation: {df[date_cols[0]].head(2).to_dict()}"
        )
        df[date_cols] = df[date_cols].apply(pd.to_datetime)
        logger.info(
            f"Two date example values after transformation: {df[date_cols[0]].head(2).to_dict()}"
        )

        # Extract date from 'startDate'
        df[Apple.DATE] = df[Apple.CREATION_DATE].dt.strftime(Settings.DATE_FORMAT)

        # Extract hour from 'startDate'
        df[Apple.HOUR] = df[Apple.START_DATE].dt.hour

        # Store original 'value' field into new column
        df[Apple.ORIGINAL_VALUE] = df[Apple.VALUE]

        # Convert 'value' to numeric, handling errors by replacing with 1.0
        df[Apple.VALUE] = pd.to_numeric(df[Apple.VALUE], errors="coerce").fillna(1.0)

        df[Apple.SOURCE_NAME] = df[Apple.SOURCE_NAME].str.strip()

        logger.info("Added new columns to 'Record' DataFrame")

        logger.info("Successfully enriched 'Record' DataFrame")

        return df

    except Exception as e:
        logger.error(f"Error occurred while enriching record DataFrame: {str(e)}")
        raise


def process_record(df):
    """
    Transforms and filters 'Record' DataFrame in provided dictionary based on apple_data information.

    Parameters:
        df: DataFrame to transform.

    Returns:
        dict: Dictionary containing filtered and transformed DataFrames.

    Raises:
        Exception: If error occurs during transformation process.
    """

    try:
        logger.info("Processing 'Record' DataFrame...")

        transformed_df = enrich_record(df)

        unique_types = set(transformed_df[Apple.TYPE_FIELD].unique())  # Unique types

        type_dfs = {}
        logger.info("Creating dictionary of DataFrames based on unique types...")

        for elem in Apple.RECORD_TYPE:
            if elem in unique_types:
                var_name = elem.replace("-", "_").replace(" ", "_")
                filtered_df = transformed_df[transformed_df[Apple.TYPE_FIELD] == elem]
                type_dfs[var_name] = filtered_df
            else:
                logger.info(f"The specified type '{elem}' does not exist in DataFrame.")

        logger.info(
            f"Dimensions of DataFrames in 'Record' dictionary: {[(key, df.shape) for key, df in type_dfs.items()]}"
        )

        logger.info("Successfully processed 'Record' DataFrame")

        return type_dfs

    except Exception as e:
        logger.error(f"Error occurred while processing 'Record' DataFrame: {str(e)}")
        raise


def subset_by_priority(df):
    """
    Subset the DataFrame by selecting rows with the minimum priority for each date.
    """
    df = df.copy()

    df[Apple.PRIORITY] = df[Apple.SOURCE_NAME].map(Apple.PRIORITY_DICT)  # Map priority
    df.sort_values(
        by=[Apple.CREATION_DATE, Apple.PRIORITY], inplace=True
    )  # Sort by date
    df = df[
        df[Apple.PRIORITY] == df.groupby(Apple.DATE)[Apple.PRIORITY].transform("min")
    ]  # Keep rows with minimum priority for each date

    return df


def handle_sleep_analysis(df):
    """
    Transforms SleepAnalysis DataFrame by aggregating sleep data.

    Parameters:
        df : DataFrame to transform.

    Returns:
        pd.DataFrame: Transformed DataFrame.
    """
    try:
        df = df.copy()

        df[Apple.ORIGINAL_VALUE] = (
            df[Apple.ORIGINAL_VALUE].str.split("SleepAnalysis").str.get(-1)
        )  # Extract sleep type

        df[Apple.DURATION] = (
            df[Apple.END_DATE] - df[Apple.START_DATE]
        ).dt.total_seconds() / 3600  # Calculate duration in hours

        pivot_df = df.pivot_table(
            index=Apple.DATE,
            columns=Apple.ORIGINAL_VALUE,
            values=Apple.DURATION,
            aggfunc="sum",
        ).reset_index()  # Pivot table

        grouped = df.groupby(Apple.DATE)  # Group by date
        pivot_df[Apple.BED_TIME] = (
            grouped[Apple.START_DATE].min().reset_index()[Apple.START_DATE]
        )  # Get bed time  by min start date
        pivot_df[Apple.AWAKE_TIME] = (
            grouped[Apple.END_DATE].max().reset_index()[Apple.END_DATE]
        )  # Get awake time by max end date
        pivot_df[Apple.SOURCE_NAME] = (
            grouped[Apple.SOURCE_NAME].first().reset_index()[Apple.SOURCE_NAME]
        )  # Get first source name for each date
        pivot_df[Apple.TIME_IN_BED] = (
            pivot_df[Apple.AWAKE_TIME] - pivot_df[Apple.BED_TIME]
        ).dt.total_seconds() / 3600  # Calculate time in bed

        pivot_df["InBed"] = pivot_df["InBed"].fillna(
            pivot_df["AsleepUnspecified"]
        )  # Fill missing values in 'InBed' with 'AsleepUnspecified'

        condition = pivot_df["Awake"].isna() & (
            pivot_df["InBed"] != pivot_df[Apple.TIME_IN_BED]
        )  # Condition to calculate 'Awake' time
        pivot_df.loc[condition, "Awake"] = (
            pivot_df[Apple.TIME_IN_BED] - pivot_df["InBed"]
        )  # Calculate 'Awake' time

        pivot_df.rename(columns={"InBed": "total_sleep"}, inplace=True)  # Rename column

        pivot_df = pivot_df[(pivot_df["Awake"] >= 0) | pd.isna(pivot_df["Awake"])]

        return pivot_df

    except Exception as e:
        logger.error(f"Error occurred in handle_sleep_analysis: {str(e)}")
        raise


def transform_record_dicts(record_dict):
    """
    Transforms record dictionaries based on instructions in apple_data DataFrame.

    Parameters:
        record_dict: Dictionary containing DataFrames to transform.

    Returns:
        Dict[str, DataFrame]: Dictionary containing transformed DataFrames.
    """

    logger.info("Transforming record dictionary...")

    new_dataframe_dict = {}

    for df_name in Apple.RECORD_TYPE:
        original_df = record_dict.get(df_name)

        if original_df is None:
            continue

        new_df = original_df.copy()

        # Extract source-related information
        source_info = Apple.RECORD_TRANSFORMATION_LOGIC.get(df_name, {})
        agg_type = source_info.get("agg_type")[0].capitalize()
        fields = source_info.get("fields")
        timeframes = source_info.get("timeframe")

        agg_dict = {}

        try:
            new_df = subset_by_priority(new_df)  # Subset DataFrame by priority

            if df_name == "MindfulSession":
                new_df.loc[:, Apple.DURATION] = (
                    new_df[Apple.END_DATE] - new_df[Apple.START_DATE]
                ).dt.total_seconds() / 60
                agg_dict.update(
                    dict(
                        zip(
                            fields,
                            [(Apple.DURATION, "sum"), (Apple.VALUE, "count")],
                        )
                    )
                )
                new_df = new_df.groupby(Apple.DATE).agg(**agg_dict).reset_index()

            elif df_name == "SleepAnalysis":
                new_df = handle_sleep_analysis(new_df)

            else:
                agg_dict = {field: (Apple.VALUE, agg_type.lower()) for field in fields}
                new_df = new_df.groupby(timeframes).agg(**agg_dict).reset_index()

            new_dataframe_dict[df_name] = (
                new_df  # Add transformed DataFrame to dictionary
            )

            logger.info(f"Successfully transformed {df_name} DataFrame in dictionary")

        except Exception as e:
            logger.error(f"Error occurred while transforming {df_name}: {str(e)}")
            raise

    logger.info("Successfully transformed record dictionary")

    return new_dataframe_dict


def join_data_by_group(complete_dict):
    """
    Join multiple DataFrames by group based on a configuration DataFrame.

    Parameters:
        complete_dict: Dictionary containing DataFrames to join.

    Returns:
        dict: Dictionary containing joined DataFrames.
    """

    logger.info("Grouping and joining DataFrames in complete_dict...")

    merger_logic = {
        **Apple.RECORD_TRANSFORMATION_LOGIC,
        **Apple.ACTIVITY_TRANSFORMATION_LOGIC,
    }

    # Create a copy of complete_dict to avoid modifying original
    complete_dict_copy = {key: df.copy() for key, df in complete_dict.items()}

    joined_data_by_group = {}

    for key in complete_dict_copy.keys():
        try:
            df_a = complete_dict_copy[key]

            for col in df_a.select_dtypes(include=["float64"]).columns:
                df_a[col] = df_a[col].apply(
                    lambda x: round(x, 2) if not pd.isna(x) and x != 0 else x
                )  # Round float columns to 2 decimal places

            if df_a.empty or key not in merger_logic:
                logger.info(
                    f"Skipping key {key} because it is empty or not in configuration."
                )
                continue

            group = merger_logic[key]["group"][0]  # Get group key

            # Initialise group key-value if it doesn't exist in joined_data_by_group
            if group not in joined_data_by_group:
                joined_data_by_group[group] = df_a.copy()
            else:
                # Outer join based on 'date' column
                joined_data_by_group[group] = pd.merge(
                    joined_data_by_group[group], df_a, how="outer", on=[Apple.DATE]
                )

        except Exception as e:
            logger.error(f"Error occurred while joining {key} DataFrame: {str(e)}")
            raise

    logger.info(
        f"Dimensions of DataFrames in complete_dict after processing: {[(key, df.shape) for key, df in joined_data_by_group.items()]}"
    )

    return joined_data_by_group


def apple_transformer():
    """
    Main function to load Apple Health CSV data, transform and standardise data,
    and save to local storage.
    """
    logger.info("!!!!!!!!!!!! apple_transformer.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()

        records = file_manager.load_file(
            FileDirectory.RAW_DATA_PATH, Apple.RECORD_DATA, "csv", low_memory=False
        )
        activity = file_manager.load_file(
            FileDirectory.RAW_DATA_PATH, Apple.ACTIVITY_DATA, "csv"
        )

        record_dict = process_record(records)
        record_dict_transformed = transform_record_dicts(record_dict)

        activity_df = process_activity(activity)
        record_dict_transformed[Apple.ACTIVITY_ELEMENT] = activity_df.copy()
        logger.info("Sucessfully created complete_dict with activity & record data")

        joined_data_by_group = join_data_by_group(record_dict_transformed)

        for key, df in joined_data_by_group.items():
            logger.info(f"Rounding floats in {key} DataFrame...")
            joined_data_by_group[key] = round_floats(df)

        for col in [Apple.BED_TIME, Apple.AWAKE_TIME]:
            df = joined_data_by_group["sleep"]
            logger.info("Cleaning sleep data...")
            logger.info(f"Stardardising {col} field...")
            logger.info(f"Sample before parsing: {df[col].head(2).to_list()}")
            df[col] = df[col].apply(
                lambda x: parse_date(x.strftime("%Y-%m-%d %H:%M:%S%z"))
            )
            logger.info(f"Sample after parsing: {df[col].head(2).to_list()}")
        df = CleanData.clean_data(df)

        for key, df in joined_data_by_group.items():
            file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, f"apple_{key}", df)

    except Exception as e:
        logger.error(f"Error occurred in apple_transformer: {str(e)}")


if __name__ == "__main__":
    apple_transformer()
