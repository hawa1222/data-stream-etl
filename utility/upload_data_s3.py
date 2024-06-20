from datetime import datetime
from io import BytesIO

import boto3

from utility.logging import setup_logging

# Initialise logging
logger = setup_logging()

# Initialize S3 client
s3_client = boto3.client("s3")
bucket_name = "etl-5h3gn2wqhzfd"


def post_data_to_s3(data, data_name):
    """
    Upload data to an S3 bucket with a structured prefix

    :param data: Data to upload (pandas DataFrame)
    :param data_name: Name of the data source
    :return: True if data was uploaded, else False
    """
    logger.info(
        f"Starting upload to S3 bucket '{bucket_name}' for data source '{data_name}'..."
    )

    try:
        # Check if the DataFrame is empty
        if data.empty:
            logger.warning(f"DataFrame '{data_name}' is empty. Upload aborted.")
            return False

        # Get the current date and time
        current_datetime = datetime.now()

        # Create the S3 object prefix with the data source name
        s3_prefix = f"{data_name.split('_')[0]}/"

        # Create the S3 object name with the data source name, date, and time
        s3_object_name = f"{s3_prefix}{data_name}_{current_datetime.strftime('%Y-%m-%d_%H-%M-%S')}.csv"

        # Create a BytesIO object to store the DataFrame data
        csv_buffer = BytesIO()

        # Write the DataFrame to the BytesIO object as CSV
        data.to_csv(csv_buffer, index=False)

        # Upload the DataFrame to S3
        s3_client.put_object(
            Bucket=bucket_name, Key=s3_object_name, Body=csv_buffer.getvalue()
        )
        logger.info(f"Succesfully uploaded data as '{s3_object_name}'")
        return True

    except Exception as e:
        logger.error(f"Error uploading DataFrame to S3: {str(e)}")
        return False


def get_data_from_s3(bucket, object_name):
    """
    Download data from an S3 bucket

    :param bucket: Bucket to download from
    :param object_name: S3 object name
    :return: Data as string, or None if an error occurred
    """
    logger.info(f"Starting download from S3 bucket: {bucket}, object: {object_name}")

    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_name)
        data = response["Body"].read().decode("utf-8")
        logger.info(f"Successfully downloaded data from {bucket}/{object_name}")
        return data
    except Exception as e:
        logger.error(f"Error downloading data from {bucket}/{object_name}: {e}")
        return None
