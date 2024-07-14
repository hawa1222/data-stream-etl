import pandas as pd

from constants import FileDirectory, Google
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()


def enrich_activities(activity_df, likes_df, subs_df):
    try:
        logger.info("Enriching activity data with likes data...")

        # Create content_title_enriched field
        activity_df[Google.CONTENT_TITLE] = activity_df.apply(
            lambda row: likes_df.loc[
                likes_df[Google.CONTENT_ID] == row[Google.CONTENT_ID],
                Google.CONTENT_TITLE,
            ].iloc[0]
            if row[Google.ACTIVITY_TYPE] == Google.ACTIVITY_TYPES[0]
            and not likes_df[
                likes_df[Google.CONTENT_ID] == row[Google.CONTENT_ID]
            ].empty
            else row[Google.CONTENT_TITLE],
            axis=1,
        )

        # Populate content_desc
        content_desc_dict = dict(
            zip(likes_df[Google.CONTENT_ID], likes_df[Google.CONTENT_DESC])
        )
        activity_df[Google.CONTENT_DESC] = activity_df[Google.CONTENT_ID].map(
            content_desc_dict
        )

        logger.info("Enriching activity data with subs data...")

        # Create dictionaries for efficient lookups
        channel_desc_dict = dict(
            zip(subs_df[Google.CHANNEL_TITLE], subs_df[Google.CHANNEL_DESC])
        )
        channel_thumbnail_dict = dict(
            zip(subs_df[Google.CHANNEL_TITLE], subs_df[Google.CHANNEL_THUMBNAIL])
        )

        # Populate channel_desc and channel_thumbnail
        activity_df[Google.CHANNEL_DESC] = activity_df[Google.CHANNEL_TITLE].map(
            channel_desc_dict
        )
        activity_df[Google.CHANNEL_THUMBNAIL] = activity_df[Google.CHANNEL_TITLE].map(
            channel_thumbnail_dict
        )

        logger.info("Adding missing rows to activity data...")

        # Add missing 'Liked' activities
        missing_likes = likes_df[~likes_df[Google.DATE].isin(activity_df[Google.DATE])]
        activity_df = pd.concat([activity_df, missing_likes], ignore_index=True)

        # Add missing 'Subscribed' activities
        missing_subs = subs_df[~subs_df[Google.DATE].isin(activity_df[Google.DATE])]
        activity_df = pd.concat([activity_df, missing_subs], ignore_index=True)

        # Sort DataFrame by published_at
        activity_df = activity_df.sort_values(Google.DATE, ascending=False).reset_index(
            drop=True
        )

        logger.info("Successfully enriched activity data")
        return activity_df

    except Exception as e:
        logger.error(f"Error occurred in enrich_activities: {str(e)}")
        raise


def youtube_activity_transformer():
    """
    Main function to merge Youtube Data from HTML and API, clean & transform, and save to local storage.
    """
    logger.info("!!!!!!!!!!!! youtube_activity_transformer.py !!!!!!!!!!!")

    try:
        file_manager = FileManager()

        likes_df = file_manager.load_file(
            FileDirectory.CLEAN_DATA_PATH, Google.LIKES_DATA
        )
        subs_df = file_manager.load_file(
            FileDirectory.CLEAN_DATA_PATH, Google.SUBS_DATA
        )
        activity_df = file_manager.load_file(
            FileDirectory.CLEAN_DATA_PATH, Google.DATA_KEY
        )

        enriched_act_df = enrich_activities(activity_df, likes_df, subs_df)

        enriched_act_df["content_id"].fillna("", inplace=True)
        enriched_act_df["channel_id"].fillna("", inplace=True)

        enriched_act_df["id"] = (
            enriched_act_df["activity_type"]
            + enriched_act_df["published_at"]
            + enriched_act_df["content_id"]
            + enriched_act_df["channel_id"]
        )

        df_name = f"{Google.DATA_KEY}_enriched"
        file_manager.save_file(FileDirectory.CLEAN_DATA_PATH, df_name, enriched_act_df)

    except Exception as e:
        logger.error(f"Error occurred in youtube_activity_transformer: {str(e)}")


if __name__ == "__main__":
    youtube_activity_transformer()
