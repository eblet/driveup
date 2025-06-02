# -*- coding: utf-8 -*-

import logging
import time
from pathlib import Path
from typing import Dict, Optional, Any, Tuple, List

# External libraries
import tqdm
import gspread
from gspread.worksheet import ValueRenderOption
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# Local imports
from . import utils # For sanitize_filename, int_to_column_letter
from . import config # Ensure config is imported
from .logger import driveup_logger

log = logging.getLogger(__name__)

# Cache for file/folder details within a single process_drive call (or sync operation)
# Needs careful handling if script becomes multi-threaded or long-running with state changes.
item_cache: Dict[str, Dict] = {}

def reconstruct_and_create_path(
    service: Resource,
    item_id: str,
    item_name: str, # Name of the current item for logs and the last path segment
    item_parents: Optional[List[str]],
    drive_id: Optional[str], # ID of the Shared Drive, or None for My Drive
    drive_backup_dir: Path, # Base backup directory for the current drive
    depth: int = 0
) -> Optional[Path]:
    """
    Recursively builds the FULL LOCAL path for item_id by querying parents via API.
    Creates local directories as needed. Uses a cache.
    Returns a Path object or None in case of error/deep recursion.
    """
    global item_cache # Allow modification of the global cache
    if depth > config.MAX_PATH_RECONSTRUCTION_DEPTH:
        log.error("Exceeded maximum recursion depth (%d) while reconstructing path for %s (%s)",
                  config.MAX_PATH_RECONSTRUCTION_DEPTH, item_name, item_id)
        return None

    # If no parents or the parent is the root of the current drive context.
    # For My Drive (drive_id is None), parents might be empty or contain the root ID (which we don't know beforehand easily).
    # For Shared Drives (drive_id is not None), the root parent ID is the drive_id itself.
    is_root = False
    if not item_parents:
        is_root = True # No parents means it's in the root (of My Drive or accessible items)
    elif drive_id and item_parents[0] == drive_id:
        is_root = True # Parent is the Shared Drive root
    # Heuristic for My Drive root: If parent is not in cache and API call fails? Risky.
    # Let's assume if it's not the Shared Drive root, we need to look up the parent.

    if is_root:
        local_path = drive_backup_dir / utils.sanitize_filename(item_name)
        # The directory will be created later if it's a folder
        # Files will have their parent dir created by download_file
        return local_path

    # If not root, there must be a parent_id
    parent_id = item_parents[0]

    # Check cache for the parent
    parent_details = item_cache.get(parent_id)

    if not parent_details:
        # Request parent details if not in cache
        try:
            log.debug("[Path] Requesting parent: %s (for %s)", parent_id, item_id)
            get_params = {
                "fileId": parent_id,
                # Request fields needed for path building: name, parents, mimeType
                "fields": "id, name, parents, mimeType",
                "supportsAllDrives": True, # Important for accessing items in Shared Drives
            }
            # Add driveId if querying within a Shared Drive context, helps disambiguate
            # This might not be strictly necessary if supportsAllDrives=True works universally?
            # Let's keep it for clarity, assuming `service` is context-aware if needed.
            # if drive_id:
            #     get_params['driveId'] = drive_id # Testing without this first

            parent_details = service.files().get(**get_params).execute()
            item_cache[parent_id] = parent_details # Cache the result
        except HttpError as e:
            log.error("[Path] API error requesting parent %s (for %s): %s", parent_id, item_id, e)
            # If parent not found (404), it might be deleted, inaccessible, or outside the current scope (e.g., MyDrive parent for Shared Drive item)
            if e.resp.status == 404:
                 log.warning("[Path] Parent %s not found. Placing item %s (%s) directly in drive backup root: %s", parent_id, item_name, item_id, drive_backup_dir)
                 # Return path in the root for this item
                 return drive_backup_dir / utils.sanitize_filename(item_name)
            return None # Other API error
        except Exception as e:
             log.error("[Path] Unknown error requesting parent %s: %s", parent_id, e)
             return None

    # Recursive call to build the parent's local path
    # Pass parent's details: id, name, parents list
    parent_local_path = reconstruct_and_create_path(
        service,
        parent_id,
        parent_details.get("name", "_unknown_parent_"), # Use cached parent's name
        parent_details.get("parents"), # Use cached parent's parents
        drive_id, # Pass the original drive context ID
        drive_backup_dir,
        depth + 1
    )

    if not parent_local_path:
        log.error("[Path] Failed to reconstruct path for parent %s of item %s (%s)", parent_id, item_name, item_id)
        # Fallback: place the current item in the drive root
        log.warning("[Path] Placing item %s (%s) in drive backup root due to parent path failure: %s", item_name, item_id, drive_backup_dir)
        return drive_backup_dir / utils.sanitize_filename(item_name)

    # Construct the full path for the current item
    current_local_path = parent_local_path / utils.sanitize_filename(item_name)

    # --- Create the parent's local directory IF it represents a folder --- #
    # This is crucial: We need the parent's *local* path to exist *before* returning the child's path,
    # especially if the child is also a folder, or if download_file expects the parent dir.
    # Check mimeType from the cached parent details.
    if parent_details.get("mimeType") == config.FOLDER_MIME_TYPE:
        if not parent_local_path.exists():
            try:
                parent_local_path.mkdir(parents=True, exist_ok=True)
                log.debug("[Path] Created local folder for parent: %s", parent_local_path)
            except OSError as e:
                log.error("[Path] Failed to create local folder for parent %s: %s", parent_local_path, e)
                # If we can't create the parent dir, we cannot place the child correctly.
                log.warning("[Path] Placing item %s (%s) in drive backup root due to parent dir creation failure: %s", item_name, item_id, drive_backup_dir)
                return drive_backup_dir / utils.sanitize_filename(item_name)
        elif not parent_local_path.is_dir():
             log.error("[Path] Expected parent path to be a directory, but it is not: %s", parent_local_path)
             # Fallback: place the current item in the drive root
             log.warning("[Path] Placing item %s (%s) in drive backup root due to parent path conflict: %s", item_name, item_id, drive_backup_dir)
             return drive_backup_dir / utils.sanitize_filename(item_name)

    return current_local_path

def download_file(
    service: Resource,
    item: Dict[str, Any],
    local_path_base: Path, # Base path (directory + sanitized name, NO extension yet)
    gspread_client: Optional[gspread.Client] = None
) -> Tuple[bool, Path]:
    """Downloads or exports a file. Returns success flag and the final path (including extension)."""
    item_id = item["id"]
    item_name = item.get("name", "_unnamed_")
    mime_type = item.get("mimeType", "")
    log_prefix = f"File '{item_name}' ({item_id})"

    request = None
    is_google_doc = False
    final_local_path = local_path_base # Start with the base path

    # Determine download/export request and final local path with extension
    if mime_type in config.GOOGLE_MIME_TYPES_EXPORT:
        is_google_doc = True
        export_info = config.GOOGLE_MIME_TYPES_EXPORT[mime_type]
        export_mime_type = export_info["mimeType"]
        # Append the correct extension for Google Docs export
        final_local_path = local_path_base.with_suffix(export_info["extension"])
        log.info("%s: Exporting as %s to %s", log_prefix, export_mime_type, final_local_path)
        request = service.files().export_media(fileId=item_id, mimeType=export_mime_type)
    elif mime_type != config.FOLDER_MIME_TYPE:
         # Regular file download, path already includes sanitized name. Extension *should* be part of the name.
         # However, Drive names might lack extensions. Let's assume `local_path_base` is sufficient.
         # If `item_name` often lacks extensions, we might need `fileExtension` field from API.
         log.info("%s: Downloading to %s", log_prefix, final_local_path)
         request = service.files().get_media(fileId=item_id, supportsAllDrives=True)
    else: # Folder
        # For folders, the `local_path_base` should already exist or be created by `reconstruct_path`.
        # Ensure it exists here just in case.
        if not local_path_base.exists():
            try:
                local_path_base.mkdir(parents=True, exist_ok=True)
                log.debug(f"{log_prefix}: Ensured local folder exists: {local_path_base}")
                driveup_logger.log_file_status(str(local_path_base), "downloaded")
            except OSError as e:
                log.error(f"{log_prefix}: Failed to create local folder {local_path_base}: {e}")
                driveup_logger.log_file_status(str(local_path_base), "failed", str(e))
                return False, local_path_base # Cannot proceed if folder creation fails
        elif not local_path_base.is_dir():
             log.error(f"{log_prefix}: Expected path for folder is not a directory: {local_path_base}")
             driveup_logger.log_file_status(str(local_path_base), "failed", "Path exists but is not a directory")
             return False, local_path_base
        return True, final_local_path # Nothing more to do for a folder

    if request is None:
        # This case should ideally not be reached if mime_type is folder or handled above
        log.warning("%s: Could not determine download/export action for MIME type '%s'", log_prefix, mime_type)
        driveup_logger.log_file_status(str(final_local_path), "skipped", f"Unknown MIME type: {mime_type}")
        return False, final_local_path

    # --- Download Block --- Ensure parent directory exists --- #
    download_success = False
    try:
        # Ensure the PARENT directory for the file exists
        parent_dir = final_local_path.parent
        if not parent_dir.exists():
            log.debug("%s: Creating parent directory: %s", log_prefix, parent_dir)
            parent_dir.mkdir(parents=True, exist_ok=True)
        elif not parent_dir.is_dir():
             log.error("%s: Parent path %s is not a directory! Cannot download file.", log_prefix, parent_dir)
             driveup_logger.log_file_status(str(final_local_path), "failed", "Parent path is not a directory")
             return False, final_local_path

        # Proceed with download/export
        with open(final_local_path, "wb") as fh:
            # Get file size for progress bar, if available (not usually for exports)
            file_size = item.get("size")
            # Only show tqdm progress bar for actual downloads with known size > 0
            use_tqdm = file_size is not None and not is_google_doc and int(file_size) > 0
            pbar = tqdm.tqdm(
                total=int(file_size) if use_tqdm else None,
                unit="B", unit_scale=True, desc=f"Downloading {final_local_path.name}", leave=False, disable=not use_tqdm
            )
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024 * 10) # 10MB chunks
            done = False
            while not done:
                # Add num_retries for robustness against transient network issues
                try:
                    status, done = downloader.next_chunk(num_retries=3)
                    if use_tqdm and status:
                        # Update progress based on resumable_progress
                        pbar.update(status.resumable_progress - pbar.n)
                except HttpError as download_err:
                    # Handle potential resumable download errors (e.g., 404 if file changed/deleted during download)
                    log.error("%s: HTTP error during download chunk: %s", log_prefix, download_err)
                    # Rethrow or handle specific statuses if needed
                    raise download_err # Reraise to be caught by the outer try block
            pbar.close()
        log.debug("%s: Successfully downloaded/exported to %s", log_prefix, final_local_path)
        driveup_logger.log_file_status(str(final_local_path), "downloaded")
        download_success = True

    except HttpError as error:
        log.error("%s: Google Drive API error (%s) during download/export to %s: %s", log_prefix, error.resp.status, final_local_path, error)
        # Clean up potentially partial file
        if final_local_path.exists():
            try: final_local_path.unlink(missing_ok=True)
            except OSError as e: log.warning(f"Could not remove partially downloaded file {final_local_path}: {e}")
        driveup_logger.log_file_status(str(final_local_path), "failed", f"API error: {error}")
        return False, final_local_path
    except IOError as e:
        log.error("%s: File system error writing %s: %s", log_prefix, final_local_path, e)
        # Clean up potentially partial file
        if final_local_path.exists():
            try: final_local_path.unlink(missing_ok=True)
            except OSError as e: log.warning(f"Could not remove partially downloaded file {final_local_path}: {e}")
        driveup_logger.log_file_status(str(final_local_path), "failed", f"I/O error: {e}")
        return False, final_local_path
    except Exception as e:
        log.error("%s: Unknown error during download to %s: %s", log_prefix, final_local_path, e, exc_info=True)
        # Clean up potentially partial file
        if final_local_path.exists():
            try: final_local_path.unlink(missing_ok=True)
            except OSError as e: log.warning(f"Could not remove partially downloaded file {final_local_path}: {e}")
        driveup_logger.log_file_status(str(final_local_path), "failed", f"Unknown error: {e}")
        return False, final_local_path

    # --- Google Sheets Post-processing --- (Only if download succeeded)
    if download_success and mime_type == "application/vnd.google-apps.spreadsheet" and gspread_client:
        log.info("%s: Exporting formulas and values from sheets...", log_prefix)
        try:
            spreadsheet = gspread_client.open_by_key(item_id)
            for worksheet in spreadsheet.worksheets():
                # Add delay to avoid hitting Sheets API quota limits
                time.sleep(config.SHEETS_API_DELAY_SECONDS)
                log.info("%s: Processing sheet '%s'", log_prefix, worksheet.title)
                worksheet_safe_name = utils.sanitize_filename(worksheet.title)
                # Create CSV paths relative to the downloaded .xlsx file
                csv_formulas_path = final_local_path.parent / f"{final_local_path.stem}.{worksheet_safe_name}.formulas.csv"
                try:
                    # Fetch both formulas and formatted values
                    formulas = worksheet.get_all_values(value_render_option=ValueRenderOption.formula)
                    formatted_values = worksheet.get_all_values(value_render_option=ValueRenderOption.formatted)
                except Exception as sheet_error:
                     log.error("%s: Failed to get data for sheet '%s': %s", log_prefix, worksheet.title, sheet_error)
                     driveup_logger.log_file_status(str(csv_formulas_path), "failed", f"Failed to get sheet data: {sheet_error}")
                     continue # Skip this sheet

                try:
                    # Check if the sheet contains formulas
                    has_formulas = False
                    formula_cells = []

                    # First, collect all cells containing formulas
                    for r_idx, row_formulas in enumerate(formulas):
                        for c_idx, cell_formula in enumerate(row_formulas):
                            # Get corresponding formatted value, handle potential index errors
                            value = ""
                            if r_idx < len(formatted_values) and c_idx < len(formatted_values[r_idx]):
                                value = formatted_values[r_idx][c_idx]

                            # Only collect cells where there is a formula (starts with '=')
                            if isinstance(cell_formula, str) and cell_formula.startswith("="):
                                has_formulas = True
                                coord = f"{utils.int_to_column_letter(c_idx + 1)}{r_idx + 1}"
                                # Escape double quotes for CSV format
                                value_escaped = value.replace('"', '""')
                                formula_escaped = cell_formula.replace('"', '""')
                                formula_cells.append((coord, formula_escaped, value_escaped))

                    # Create the CSV file only if formulas are present
                    if has_formulas:
                        with open(csv_formulas_path, "w", encoding="utf-8", newline="") as f_csv:
                            # Write header
                            f_csv.write("Cell,Formula,FormattedValue\n")
                            # Write all collected formula cells
                            for coord, formula, value in formula_cells:
                                f_csv.write(f'{coord},"{formula}","{value}"\n')

                        log.info("%s: Sheet '%s' formulas saved to %s", log_prefix, worksheet.title, csv_formulas_path)
                        driveup_logger.log_file_status(str(csv_formulas_path), "downloaded")
                    else:
                        log.info("%s: Sheet '%s' has no formulas, skipping CSV creation", log_prefix, worksheet.title)
                        driveup_logger.log_file_status(str(csv_formulas_path), "skipped", "No formulas found")

                except IOError as io_err:
                    log.error("%s: Error writing formula CSV file %s: %s", log_prefix, csv_formulas_path, io_err)
                    driveup_logger.log_file_status(str(csv_formulas_path), "failed", f"I/O error: {io_err}")
                except Exception as e:
                    log.error("%s: Unknown error writing formulas CSV for sheet '%s': %s", log_prefix, worksheet.title, e, exc_info=True)
                    driveup_logger.log_file_status(str(csv_formulas_path), "failed", f"Unknown error: {e}")

        except HttpError as sheet_error:
             # Handle specific API errors like permission denied (403)
            if sheet_error.resp.status == 403:
                log.error("%s: Access denied (403) opening sheet '%s'. Check permissions.", log_prefix, item_name)
                driveup_logger.log_file_status(str(final_local_path), "failed", "Access denied (403)")
            else:
                log.error("%s: Google Sheets API error (%s) for sheet '%s': %s", log_prefix, sheet_error.resp.status, item_name, sheet_error)
                driveup_logger.log_file_status(str(final_local_path), "failed", f"Sheets API error: {sheet_error}")
        except gspread.exceptions.APIError as gspread_error:
            log.error("%s: gspread API error for sheet '%s': %s", log_prefix, item_name, gspread_error)
            driveup_logger.log_file_status(str(final_local_path), "failed", f"gspread API error: {gspread_error}")
        except Exception as e:
            log.error("%s: Unknown error processing sheet '%s': %s", log_prefix, item_name, e, exc_info=True)
            driveup_logger.log_file_status(str(final_local_path), "failed", f"Unknown error processing sheet: {e}")

    # Removed S3 upload block for general files

    # Return download success status and the final path (which includes extension)
    return download_success, final_local_path 