from extractors.apple_extractor import apple_extractor
from extractors.daylio_extractor import daylio_extractor
from extractors.spend_extractor import spend_extractor
from extractors.strava_extractor import strava_extractor
from loaders.apple_loader import apple_loader
from loaders.daylio_loader import daylio_loader
from loaders.spend_loader import spend_loader
from loaders.strava_loader import strava_loader
from loaders.youtube_loader import youtube_loader
from transformers.apple_transformer import apple_transformer
from transformers.daylio_transformer import daylio_transformer
from transformers.spend_transformer import spend_transformer
from transformers.strava_transformer import strava_transformer
from transformers.youtube_activity_transformer import youtube_activity_transformer
from utility.log_manager import setup_logging
from validation.post_load_checks import post_load

logger = setup_logging()


def main():
    try:
        apple_extractor()
        apple_transformer()
        apple_loader()

        strava_extractor()
        strava_transformer()
        strava_loader()

        youtube_activity_transformer()
        youtube_loader()

        daylio_extractor()
        daylio_transformer()
        daylio_loader()

        spend_extractor()
        spend_transformer()
        spend_loader()

        post_load()

    except Exception as e:
        logger.error(f"Error occurred in main: {str(e)}")


if __name__ == "__main__":
    main()
