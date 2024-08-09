"""
Script to connect to Redis and manage caching of data.
"""

import json

from datetime import datetime, timedelta

import pandas as pd
import redis

from config import REDIS_DB, REDIS_HOST, REDIS_PASSWORD, REDIS_PORT
from utility.log_manager import setup_logging

logger = setup_logging()

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    db=REDIS_DB,
    decode_responses=True,
    socket_connect_timeout=10,
    socket_timeout=10,
)


def get_cached_ids(cache_key):
    """
    Initialise the cache by retrieving the cached data for a given key.

    Parameters:
        cache_key (str): The key to retrieve the cached data from Redis.

    Returns:
        set: An empty set if the key doesn't exist, or a set containing the cached data.
    """
    logger.debug(f"Fetching '{cache_key}' cache from Redis...")

    try:
        cached_data = redis_client.smembers(cache_key)
        logger.info(f"Successfully fetched {len(cached_data)} IDs from '{cache_key}' cache")
        return {item if isinstance(item, str) else item.decode() for item in cached_data}
    except Exception as e:
        logger.error(f"Error fetching '{cache_key}' cache: {str(e)}")
        raise


def update_cached_ids(cache_key, ids):
    """
    Update the cache by adding new data to the existing cached data for a given key.

    Parameters:
        cache_key (str): The key to update the cached data in Redis.
        new_data (set): A set containing the new data to be added to the cache.
    """
    logger.debug(f"Updating '{cache_key}' cache in Redis...")

    try:
        redis_client.sadd(cache_key, *ids)
        logger.info(f"Successfully updated '{cache_key}' cache, added {len(ids)} new IDs")
    except Exception as e:
        logger.error(f"Error updating '{cache_key}' cache: {str(e)}")
        raise


def get_cached_data(cache_key, expiry_time=48):
    logger.debug(f"Fetching '{cache_key}' cache from Redis...")

    try:
        cached_data = redis_client.get(cache_key)

        if cached_data is None:
            redis_client.setex(cache_key, timedelta(hours=expiry_time), json.dumps({}))
            logger.info(f"'{cache_key}' cache not found. Empty cache created")
            return None
        cached_data = json.loads(cached_data)
        timestamp = cached_data.get("timestamp")

        if timestamp:
            cache_time = datetime.fromisoformat(timestamp)
            if datetime.now() - cache_time < timedelta(hours=expiry_time):
                logger.info(f"'{cache_key}' cache found, timestamp within {expiry_time} hours")
                return cached_data.get("data")

        logger.info(f"'{cache_key}' cache found, timestamp older than {expiry_time} hours")
        return None

    except Exception as e:
        logger.error(f"Error fetching cached data: {str(e)}")
        raise


class JSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that converts Timestamps to strings.
    """

    def default(self, obj):
        if isinstance(obj, datetime | pd.Timestamp):
            return obj.isoformat()  # Convert datetime to string
        return json.JSONEncoder.default(self, obj)


def update_cached_data(cache_key, data):
    logger.debug(f"Updating '{cache_key}' cache in Redis...")
    """
    Update the cache with new data for the given key.

    :param key: Unique identifier for the cache entry
    :param data: Data to be cached
    """
    try:
        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient="records")

        cache_data = {"timestamp": datetime.now().isoformat(), "data": data}
        redis_client.setex(
            cache_key, timedelta(hours=48), json.dumps(cache_data, cls=JSONEncoder, indent=2)
        )
        logger.info(f"Succesfully updated '{cache_key}' cache, {len(data)} total entries")

    except Exception as e:
        logger.error(f"Error updating '{cache_key}' cache: {str(e)}")
        raise


def delete_cache_values(key, values):
    """
    Delete specific values from a Redis key based on their data type.

    Parameters:
        redis_client: The Redis client connection.
        key: The key under which values are stored.
        values: The values to be deleted.
        data_type: Optional. The data type of key. If not provided, it will be determined.
    """
    logger.debug(f"Deleting values from key: {key}")

    if not redis_client.exists(key):
        logger.error(f"Key {key} does not exist.")
        return

    data_type = redis_client.type(key)

    if data_type == "list":
        for value in values:
            redis_client.lrem(key, 0, value)
        logger.info(f"Successfully deleted {len(values)} values from list")
    elif data_type == "set":
        deleted_count = redis_client.srem(key, *values)
        logger.info(f"Successfully deleted {deleted_count} values from set")
    elif data_type == "zset":
        deleted_count = redis_client.zrem(key, *values)
        logger.info(f"Successfully deleted {deleted_count} values from sorted set")
    elif data_type == "hash":
        deleted_count = redis_client.hdel(key, *values)
        logger.info(f"Successfully deleted {deleted_count} values from hash")
    else:
        logger.error(f"Unsupported data type for deletion: {data_type} or key does not exist.")
