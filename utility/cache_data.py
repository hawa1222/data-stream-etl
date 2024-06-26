"""
This module provides utility functions for caching data using Redis.

It includes functions to initialise the cache, update the cache with new data,
and retrieve cached data based on specific keys.
"""

import json
from datetime import datetime, timedelta

import redis

from utility.logging import setup_logging

logger = setup_logging()

# Establish a connection to Redis
redis_client = redis.Redis(
    host="localhost", port=6379, db=0, password=None, decode_responses=True
)


def get_cached_ids(cache_key):
    """
    Initialise the cache by retrieving the cached data for a given key.

    Args:
        cache_key (str): The key to retrieve the cached data from Redis.

    Returns:
        set: An empty set if the key doesn't exist, or a set containing the cached data.
    """
    logger.info(f"Fetching '{cache_key}' cache from Redis...")

    try:
        cached_data = redis_client.smembers(cache_key)
        logger.info(f"Successfully fetched {len(cached_data)} IDs")
        return {
            item if isinstance(item, str) else item.decode() for item in cached_data
        }
    except Exception as e:
        logger.error(f"Error fetching cached IDs: {str(e)}")
        return set()


def update_cached_ids(cache_key, ids):
    """
    Update the cache by adding new data to the existing cached data for a given key.

    Args:
        cache_key (str): The key to update the cached data in Redis.
        new_data (set): A set containing the new data to be added to the cache.
    """
    logger.info(f"Updating '{cache_key}' cache in Redis...")

    try:
        redis_client.sadd(cache_key, *ids)
        logger.info(f"Successfully updated cached IDs, added {len(ids)} new IDs")
    except Exception as e:
        logger.error(f"Error updating cached IDs: {str(e)}")


expiry_time = 48


def get_cached_data(cache_key):
    logger.info(f"Fetching '{cache_key}' cache from Redis...")

    try:
        cached_data = redis_client.get(cache_key)

        if cached_data is None:
            redis_client.setex(cache_key, timedelta(hours=expiry_time), json.dumps({}))
            logger.info("Cache not found. Empty cache created")
            return None
        cached_data = json.loads(cached_data)
        timestamp = cached_data.get("timestamp")

        if timestamp:
            cache_time = datetime.fromisoformat(timestamp)
            if datetime.now() - cache_time < timedelta(hours=expiry_time):
                logger.info(f"Cache found, timestamp within {expiry_time} hours")
                return cached_data.get("data")

        logger.info(f"Cache found, timestamp older than {expiry_time} hours")
        return None

    except Exception as e:
        logger.error(f"Error fetching cached data: {str(e)}")
        return None


def update_cached_data(cache_key, data):
    logger.info(f"Updating '{cache_key}' cache in Redis...")
    """
    Update the cache with new data for the given key.

    :param key: Unique identifier for the cache entry
    :param data: Data to be cached
    """
    try:
        cache_data = {"timestamp": datetime.now().isoformat(), "data": data}
        redis_client.setex(cache_key, timedelta(hours=48), json.dumps(cache_data))
        logger.info(f"Succesfully updated cache, {len(data)} total entries")

    except Exception as e:
        logger.error(f"Error updating cached data: {str(e)}")
