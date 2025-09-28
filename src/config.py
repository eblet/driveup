# -*- coding: utf-8 -*-

import logging
import os
from pathlib import Path

# --- Try importing python-dotenv ---
try:
    import dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False
    dotenv = None # Placeholder

# --- Try importing Boto3 ---
try:
    import boto3
    from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None # Placeholder
    # Define dummy exceptions for except blocks if boto3 is not installed
    NoCredentialsError = type('NoCredentialsError', (Exception,), {})
    PartialCredentialsError = type('PartialCredentialsError', (Exception,), {})
    ClientError = type('ClientError', (Exception,), {})

import coloredlogs

# --- Load Environment Variables ---
if DOTENV_AVAILABLE:
    dotenv.load_dotenv()
    print("Loaded environment variables from .env")
else:
    print("Warning: python-dotenv not installed. Using default configuration values or environment variables.")

# --- Configuration --- (Load from .env or use defaults)
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")
TOKEN_FILE = os.getenv("TOKEN_FILE", "token.json")
BASE_DOWNLOAD_DIR = Path(os.getenv("BASE_DOWNLOAD_DIR", "google_drive_backup"))
SHARED_FILES_DIR_NAME = os.getenv("SHARED_FILES_DIR_NAME", "Shared With Me")
SHARED_FILES_DIR = BASE_DOWNLOAD_DIR / SHARED_FILES_DIR_NAME # Derived, but defined here for clarity
ARCHIVE_DIR = Path(os.getenv("ARCHIVE_DIR", "google_drive_archives"))
STATE_DIR = Path(os.getenv("STATE_DIR", "backup_state"))

# State filenames (relative to STATE_DIR/drive_name/)
STATE_MAP_FILENAME = os.getenv("STATE_MAP_FILENAME", "drive_state.json")
STATE_MAP_DRY_RUN_FILENAME = os.getenv("STATE_MAP_DRY_RUN_FILENAME", "drive_state_dry_run.json")
START_TOKEN_FILENAME = os.getenv("START_TOKEN_FILENAME", "start_token.txt")

# Load numeric values with defaults and type casting
def get_int_env(key, default):
    val = os.getenv(key)
    if val is None:
        print(f"Warning: Environment variable {key} not set, using default value {default}.")
        return default
    try:
        return int(val)
    except ValueError:
        print(f"Warning: Invalid value '{val}' for {key} in environment, using default value {default}.")
        return default

SHEETS_API_DELAY_SECONDS = get_int_env("SHEETS_API_DELAY_SECONDS", 2)
MAX_PATH_RECONSTRUCTION_DEPTH = get_int_env("MAX_PATH_RECONSTRUCTION_DEPTH", 20)
DRY_RUN_SAMPLE_SIZE = get_int_env("DRY_RUN_SAMPLE_SIZE", 10)

dry_run_max_size_mb = get_int_env("DRY_RUN_MAX_FILE_SIZE_MB", 1)
DRY_RUN_MAX_FILE_SIZE_BYTES = dry_run_max_size_mb * 1024 * 1024

# Google Drive export size limits (approximate)
max_export_size_mb = get_int_env("MAX_EXPORT_SIZE_MB", 50)  # Google Docs export limit is around 50MB
MAX_EXPORT_SIZE_BYTES = max_export_size_mb * 1024 * 1024

# --- Logging Setup ---
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
valid_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
if log_level_str not in valid_log_levels:
    print(f"Warning: Invalid LOG_LEVEL '{log_level_str}' in environment. Using default level INFO.")
    log_level_str = "INFO"

# Configure coloredlogs
coloredlogs.install(level=log_level_str, fmt="%(asctime)s %(levelname)s %(message)s")

# Get the root logger
log = logging.getLogger(__name__) # Use __name__ for the logger in this module
log.info("Logger configured with level %s", log_level_str)

# --- Google MIME Types --- (Keep constants related to API interactions here)
GOOGLE_MIME_TYPES_EXPORT = {
    "application/vnd.google-apps.document": {
        "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "extension": ".docx",
    },
    "application/vnd.google-apps.spreadsheet": {
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "extension": ".xlsx",
    },
    "application/vnd.google-apps.presentation": {
        "mimeType": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "extension": ".pptx",
    },
    "application/vnd.google-apps.drawing": {
        "mimeType": "image/png",
        "extension": ".png",
    },
    # Add other Google Workspace types and their export formats if needed
}
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
GOOGLE_DOCS_MIMETYPES = set(GOOGLE_MIME_TYPES_EXPORT.keys()) # Set for quick lookup 