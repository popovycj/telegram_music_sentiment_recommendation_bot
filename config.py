import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

DB_HOST     = os.getenv("DB_HOST")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

SUPPORTED_LANGS = ['en', 'uk']
MAX_REQUESTS    = 5
TIME_WINDOW     = 60  # seconds
