# import json

# import pandas as pd
# import redis

# from utility import cache

# # def load_excel_data(file_path):
# #     df = pd.read_excel(file_path)
# #     return df


# # # Example usage
# # excel_file_path = "/Users/hadid/GitHub/ETL/data/archive/raw_data/strava_data.xlsx"
# # strava_df = load_excel_data(excel_file_path)
# # # strava_json = strava_df.to_json(orient='records')
# # # print(strava_json[:1000])
# # strava_dict = strava_df.to_dict(orient="records")
# # print(strava_dict[:1])


# # Establish a connection to Redis
# redis_client = redis.Redis(
#     host="localhost", port=6379, db=0, password=None, decode_responses=True
# )


# # def cache_strava_activity_data(cache_key, activity_data):
# #     # Convert activity_data to JSON format
# #     activity_data_json = json.dumps(activity_data)

# #     # Store the activity data in Redis hash using the 'id' field as the key
# #     for activity in activity_data:
# #         activity_id = activity["id"]
# #         redis_client.hset(cache_key, activity_id, activity_data_json)


# def get_strava_activity_data(cache_key):
#     # Retrieve all activity data from the Redis hash
#     activity_data_json = redis_client.hgetall(cache_key)

#     # Deserialize the JSON data back to a dictionary
#     activity_data = {}
#     for activity_id, data_json in activity_data_json.items():
#         activity_data[activity_id.decode("utf-8")] = json.loads(data_json)

#     return activity_data


# # Load existing data
# try:
#     existing_data = pd.read_csv("activity_data.csv")
# except FileNotFoundError:
#     existing_data = pd.DataFrame()


# cache_key = "strava_activity_data"
# # cache_strava_activity_data(cache_key, strava_dict)

# cached_data = get_strava_activity_data(cache_key)
# print(cached_data[:2])

import pandas as pd

data_path = "/Users/hadid/GitHub/ETL/data/raw_data/strava/strava_data.xlsx"
data_path_2 = "/Users/hadid/GitHub/ETL/data/raw_data/strava/strava_data_v2.xlsx"

# Load existing data
try:
    existing_data = pd.read_excel(data_path)
except FileNotFoundError:
    existing_data = pd.DataFrame()

# Combine existing data with new data
combined_data = pd.concat([existing_data, existing_data], ignore_index=True)

# Save combined data to CSV
combined_data.to_excel(data_path_2, index=False)


existing_data = pd.read_excel(data_path)

from utility import cache_data

Cache new activity IDs in Redis
cache_data.cach_ids("strava_activity_ids", all_activity_ids)
