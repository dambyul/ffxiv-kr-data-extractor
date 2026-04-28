import os
from dotenv import load_dotenv

# Root is 3 levels up: transform/lib/config.py -> transform/lib -> transform -> [Project Root]
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables from .env file in the project root
load_dotenv(os.path.join(BASE_DIR, ".env"))

class Config:
    BASE_DIR = BASE_DIR

    # Google Sheets
    GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
    GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "google_sheet.json")
    if not os.path.isabs(GOOGLE_CREDS_PATH):
        GOOGLE_CREDS_PATH = os.path.join(BASE_DIR, GOOGLE_CREDS_PATH)

    # AWS S3
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

    # Discord
    DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
    DISCORD_USER_ID = os.getenv("DISCORD_USER_ID", "")
    DISCORD_AVATAR_URL = os.getenv("DISCORD_AVATAR_URL", "")
    DISCORD_USERNAME = os.getenv("DISCORD_USERNAME", "FFXIV Extractor")

