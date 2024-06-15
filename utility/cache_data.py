"""
This module provides utility functions for caching data using Redis.

It includes functions to initialise the cache, update the cache with new data,
and retrieve cached data based on specific keys.
"""

import json

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
    logger.info(f"Fetching cached {cache_key} from Redis...")

    try:
        cached_data = redis_client.smembers(cache_key)
        logger.info(f"Successfully fetched {len(cached_data)} IDs")
        return {
            item if isinstance(item, str) else item.decode() for item in cached_data
        }
    except Exception as e:
        logger.error(f"Error retrieving cached data. Error: {str(e)}")
        return set()


def cache_ids(cache_key, ids):
    """
    Update the cache by adding new data to the existing cached data for a given key.

    Args:
        cache_key (str): The key to update the cached data in Redis.
        new_data (set): A set containing the new data to be added to the cache.
    """
    logger.info(f"Updating cached {cache_key} in Redis...")

    try:
        redis_client.sadd(cache_key, *ids)
        logger.info(
            f"Successfully updated cached {cache_key}, added {len(ids)} new IDs"
        )
    except Exception as e:
        logger.error(f"Error updating cache for key '{cache_key}'. Error: {str(e)}")


def cache_data(cache_key, data):
    """
    Cache the provided data in Redis using the specified key.

    Args:
        cache_key (str): The key to store the data in Redis.
        data (dict): The data to be cached.
    """
    redis_client.set(cache_key, json.dumps(data))


def get_cached_data(cache_key):
    """
    Retrieve the cached data from Redis for a given key.

    Args:
        cache_key (str): The key to retrieve the cached data from Redis.

    Returns:
        dict: The cached data as a dictionary, or None if the key doesn't exist.
    """
    cached_data = redis_client.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    return None
