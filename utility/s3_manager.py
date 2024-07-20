"""
Script to manage uploading and downloading data to/from an S3 bucket
"""

from datetime import datetime
from io import BytesIO

import boto3

from config import S3_BUCKET
from utility.log_manager import setup_logging

logger = setup_logging()

s3_client = boto3.client("s3")  # Create S3 client


def post_data_to_s3(object_name, data, overwrite=False):
    """
    Upload data to an S3 bucket with a structured prefix

    :param data: Data to upload (pandas DataFrame)
    :param object_name: Name of data source
    :return: True if data was uploaded, else False
    """
    logger.debug(f"Starting upload to S3 bucket '{S3_BUCKET}' for data source '{object_name}'...")

    try:
        # Check if DataFrame is empty
        if data.empty:
            logger.warning(f"DataFrame '{object_name}' is empty. Upload aborted.")
            return False

        # Get current date and time
        current_datetime = datetime.now()

        # Create S3 object prefix with data source name
        s3_prefix = f"{object_name.split('_')[0]}/"

        if overwrite:
            # Create S3 object name with data source name
            s3_object_name = f"{s3_prefix}{object_name}.csv"
        else:
            # Create S3 object name with data source name, date, and time
            s3_object_name = (
                f"{s3_prefix}{object_name}_{current_datetime.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            )

        # Create a BytesIO object to store DataFrame data
        csv_buffer = BytesIO()

        # Write DataFrame to BytesIO object as CSV
        data.to_csv(csv_buffer, index=False)

        # Upload DataFrame to S3
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_object_name, Body=csv_buffer.getvalue())
        logger.info(
            f"Succesfully uploaded data to S3 bucket '{S3_BUCKET}', object '{s3_object_name}'"
        )
        return True

    except Exception as e:
        logger.error(f"Error uploading DataFrame to S3 bucket '{S3_BUCKET}': {str(e)}")
        return False


def get_data_from_s3(object_name):
    """
    Download data from an S3 bucket

    :param bucket: Bucket to download from
    :param object_name: S3 object name
    :return: Data as string, or None if an error occurred
    """
    logger.debug(f"Starting download from S3 bucket '{S3_BUCKET}', object '{object_name}'...")

    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=object_name)
        data = response["Body"].read().decode("utf-8")
        logger.info(
            f"Successfully downloaded data from S3 bucket '{S3_BUCKET}', object '{object_name}'"
        )
        return data
    except Exception as e:
        logger.error(f"Error downloading data from S3 bucket '{S3_BUCKET}': {str(e)}")
        return None
