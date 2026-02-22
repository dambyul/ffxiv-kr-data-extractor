import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    # Google Sheets
    GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
    GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "google_sheet.json")

    # AWS S3
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

    # Discord
    DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
    DISCORD_USER_ID = os.getenv("DISCORD_USER_ID", "")
    DISCORD_AVATAR_URL = os.getenv("DISCORD_AVATAR_URL", "")
    DISCORD_USERNAME = os.getenv("DISCORD_USERNAME", "FFXIV Extractor")

    # Paths
    # Paths - Derived relative to this file (transform/lib/config.py)
    # Root is 3 levels up: transform/lib/config.py -> transform/lib -> transform -> [Project Root]
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
