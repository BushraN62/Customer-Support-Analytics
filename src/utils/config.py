"""
Configuration settings for the application
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Database
DATABASE_URL = os.getenv('DATABASE_URL')

# App settings
APP_NAME = os.getenv('APP_NAME', 'CX Insights Lab')
DEBUG = os.getenv('DEBUG', 'True') == 'True'

# File upload limits
MAX_FILE_SIZE_MB = 50
ALLOWED_EXTENSIONS = ['csv', 'xlsx']

# Analysis settings
DEFAULT_NUM_THEMES = 8
MIN_THEMES = 5
MAX_THEMES = 15

# Date format
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'