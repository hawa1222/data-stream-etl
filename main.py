# =============================================================================
# # Import the required libraries
# import sys
# 
# # Configuration
# sys.dont_write_bytecode = True  # Prevent Python from writing bytecode files (.pyc)
# sys.path.append('/Users/hadid/GitHub/ETL')  # Add path to system path
# =============================================================================

# Custom imports
from extractors.apple_extractor import apple_extractor
from extractors.daylio_extractor import daylio_extractor
from extractors.spend_extractor import spend_extractor
from extractors.strava_extractor import strava_extractor
from extractors.youtube_html_extractor import youtube_html_extractor
from extractors.youtube_extractor import youtube_extractor

from transformers.apple_transformer import apple_transformer
from transformers.daylio_transformer import daylio_transformer
from transformers.spend_transformer import spend_transformer
from transformers.strava_transformer import strava_transformer
from transformers.youtube_html_transformer import youtube_html_transformer
from transformers.youtube_transformer import youtube_transformer

from loaders.apple_loader import apple_loader
from loaders.daylio_loader import daylio_loader
from loaders.spend_loader import spend_loader
from loaders.strava_loader import strava_loader
from loaders.youtube_loader import youtube_loader

from validation.post_load_checks import post_load

# Initialise logging
from utility.logging import setup_logging
setup_logging()


##################################################################################################################################

def main():
    
    apple_extractor()
    apple_transformer()
    apple_loader()

    daylio_extractor()
    daylio_transformer()
    daylio_loader()
    
    spend_extractor()
    spend_transformer()
    spend_loader()

    strava_extractor()
    strava_transformer()
    strava_loader()

    youtube_html_extractor()
    youtube_html_transformer()
    
    youtube_extractor()
    youtube_transformer()
    youtube_loader()
    
    post_load()

if __name__ == "__main__":
    main()
