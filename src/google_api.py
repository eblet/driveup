# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import Optional, Tuple

# Google Auth Libraries
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError

# Try importing gspread for type hinting/access
try:
    import gspread
except ImportError:
    gspread = None # Placeholder if not installed

# Import config constants
from . import config

log = logging.getLogger(__name__)

# --- API and File Processing Functions ---


def create_service_clients_from_creds(creds: Credentials) -> Tuple[Resource, Optional[gspread.Client]]:
    """
    Create thread-safe Google Drive and Sheets API clients from credentials.

    Args:
        creds: Google API credentials.

    Returns:
        A tuple containing the Drive service resource and gspread client.
    """
    drive_service = build('drive', 'v3', credentials=creds)
    gspread_client = None
    try:
        gspread_client = gspread.authorize(creds)
    except Exception as e:
        log.warning(f"Failed to initialize Google Sheets API client in thread: {e}")
    
    return drive_service, gspread_client


def get_credentials(token_path: Path = Path(config.TOKEN_FILE), creds_path: Path = Path(config.CREDENTIALS_FILE)) -> Optional[Credentials]:
    """Gets valid Google API credentials, refreshing or initiating OAuth flow if necessary."""
    creds = None
    # token_path = Path(config.TOKEN_FILE) # This line is now redundant as token_path is passed as an argument
    # creds_path = Path(config.CREDENTIALS_FILE) # This line is now redundant as creds_path is passed as an argument

    if token_path.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(token_path), config.SCOPES)
            log.info("Credentials loaded from %s", token_path)
        except Exception as e:
            log.warning("Failed to load token from %s: %s. Re-authorization needed.", token_path, e)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                log.info("Refreshing access token...")
                creds.refresh(Request())
            except Exception as e:
                log.error("Failed to refresh token: %s. Re-authorization needed.", e)
                creds = None # Reset creds to trigger the flow below

        # If still no valid creds, start the OAuth flow
        if not creds:
             log.info("Requesting new access token...")
             if not creds_path.exists():
                 log.critical("Credentials file %s not found. Cannot obtain credentials.", creds_path)
                 return None
             try:
                 # Require offline access to get a refresh token
                 flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), config.SCOPES)
                 # Specify access_type='offline' to ensure a refresh token is included
                 creds = flow.run_local_server(port=0, prompt='consent', access_type='offline')
             except Exception as e:
                  log.critical("Error during OAuth authorization process: %s", e)
                  return None

        # Save the credentials for the next run
        if creds:
            try:
                with open(token_path, "w") as token_file_handle:
                    token_file_handle.write(creds.to_json())
                log.info("Access token saved to %s", token_path)
            except Exception as e:
                log.error("Failed to save token to %s: %s", token_path, e)
        else:
             log.error("Failed to obtain or refresh token.")
             return None # Explicitly return None if auth failed

    return creds

# Expose build and HttpError for convenience in main module
__all__ = ['get_credentials', 'build', 'HttpError', 'gspread', 'Resource'] 