# -*- coding: utf-8 -*-

import json
import logging
from pathlib import Path
from typing import Dict, Optional, Any

from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

# Import config constants if needed (e.g., for logging)
# from . import config

log = logging.getLogger(__name__)

# --- State Management ---

def load_drive_state(state_file: Path) -> Dict[str, Dict[str, Any]]:
    """
    Loads the state map {fileId: {path, modifiedTime, is_folder}}.
    Handles both old format (dict) and new format ({"total_size_bytes":..., "items":{...}}).
    """
    if state_file.exists():
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Check for new format (used in dry-run full sync)
            if isinstance(data, dict) and "items" in data and "total_size_bytes" in data:
                total_size = data.get("total_size_bytes")
                is_estimated = data.get("google_docs_estimated", False)
                log.info(
                    f"Loaded drive state from {state_file}. Recorded total size (dry-run): {total_size} bytes "
                    f"({'Google Docs estimated/excluded' if is_estimated else ''})."
                )
                state_map = data["items"]
            # Assume old format or normal run
            elif isinstance(data, dict):
                 state_map = data
            else:
                 # Should not happen if saved correctly, but handle unexpected format
                 log.warning(f"State map file {state_file} has unexpected format. Full sync required.")
                 return {}

            log.info("Drive state map ('items' part) loaded from %s (%d entries)", state_file, len(state_map))
            return state_map

        except json.JSONDecodeError:
            log.warning("State map file %s is corrupted. Full sync required.", state_file)
        except Exception as e:
            log.error("Failed to read state map file %s: %s. Full sync required.", state_file, e)
    else:
        log.info("State map file %s not found. Full sync required.", state_file)
    return {}

def save_drive_state(
    state_data: Dict[str, Dict[str, Any]],
    state_file: Path,
    total_size_bytes: Optional[int] = None, # Add optional total size
    google_docs_estimated: bool = False     # Add flag for size estimation
):
    """
    Saves the drive state map.
    If total_size_bytes is provided (only during dry-run full sync),
    saves a structured JSON with size and items. Otherwise, saves only the state map.
    """
    try:
        state_file.parent.mkdir(parents=True, exist_ok=True)
        # Prepare data to be saved based on whether total_size is provided
        data_to_save: Any
        if total_size_bytes is not None:
            data_to_save = {
                "total_size_bytes": total_size_bytes,
                "google_docs_estimated": google_docs_estimated,
                "items": state_data
            }
            log_msg = f"Drive state map and total size ({total_size_bytes} bytes) saved to {state_file}"
        else:
            data_to_save = state_data
            log_msg = f"Drive state map saved to {state_file} ({len(state_data)} entries)"

        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(data_to_save, f, indent=2)
        log.info(log_msg)

    except Exception as e:
        log.error("Failed to save drive state to %s: %s", state_file, e)


# --- Token Management ---
def load_start_page_token(token_file: Path) -> Optional[str]:
    """Loads the startPageToken from a file."""
    if token_file.exists():
        try:
            with open(token_file, "r") as f:
                token = f.read().strip()
                if token:
                    log.info("StartPageToken loaded from %s", token_file)
                    return token
                else:
                    log.warning("StartPageToken file %s is empty.", token_file)
        except Exception as e:
            log.error("Failed to read StartPageToken file %s: %s", token_file, e)
    else:
         log.info("StartPageToken file %s not found.", token_file)
    return None

def save_start_page_token(token: str, token_file: Path):
    """Saves the startPageToken to a file."""
    try:
        token_file.parent.mkdir(parents=True, exist_ok=True)
        with open(token_file, "w") as f:
            f.write(token)
        log.info("StartPageToken saved to %s", token_file)
    except Exception as e:
        log.error("Failed to save StartPageToken to %s: %s", token_file, e)

def get_initial_start_page_token(service: Resource, drive_id: Optional[str] = None) -> Optional[str]:
    """Gets the initial startPageToken for the changes API."""
    try:
        token_params = {"supportsAllDrives": True}
        if drive_id:
            token_params["driveId"] = drive_id
        response = service.changes().getStartPageToken(**token_params).execute()
        token = response.get("startPageToken")
        if token:
            log.info("Obtained initial StartPageToken%s: %s", f" for driveId {drive_id}" if drive_id else "", token)
            return token
        else:
             log.error("Failed to get initial StartPageToken%s.", f" for driveId {drive_id}" if drive_id else "")
             return None
    except HttpError as e:
        log.error("API error getting initial StartPageToken%s: %s", f" for driveId {drive_id}" if drive_id else "", e)
        return None 