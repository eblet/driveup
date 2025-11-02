# -*- coding: utf-8 -*-

import logging
import ssl
import time
from pathlib import Path
from typing import Dict, Optional, Any, Set, Tuple
import random

from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError
import gspread

from . import config
from . import utils
from . import state_manager
from . import file_processor
from . import rate_limiter

log = logging.getLogger(__name__)

def process_drive(
    drive_service: Resource,
    gspread_client: Optional[gspread.Client],
    drive_id: Optional[str],
    drive_name: str,
    drive_backup_dir: Path,
    drive_state_dir: Path,
    processed_shared_drive_ids: Set[str],
    incremental_flag: bool,
    dry_run: bool
) -> Tuple[int, int, int, int, str]:
    """
    Process a single drive (My Drive or Shared Drive).
    Returns (processed_count, downloaded_count, deleted_count, failed_count, actual_mode).
    """
    log.info(f"Processing drive: {drive_name} (ID: {drive_id if drive_id else 'My Drive'})")
    
    # Initialize counters
    processed_count = 0
    downloaded_count = 0
    deleted_count = 0
    failed_count = 0
    shortcuts_skipped_count = 0  # Track skipped Google Shortcuts
    
    # Setup state files
    state_file = drive_state_dir / (config.STATE_MAP_DRY_RUN_FILENAME if dry_run else config.STATE_MAP_FILENAME)
    token_file = drive_state_dir / config.START_TOKEN_FILENAME
    
    # Load state map. If incremental, also try to load token.
    state_map = state_manager.load_drive_state(state_file)
    start_token = None
    needs_full_sync = False
    
    if incremental_flag:
        start_token = state_manager.load_start_page_token(token_file)
        if not start_token:
            log.warning(f"No start token found for {drive_name}. Performing full sync.")
            needs_full_sync = True
            # Clear potentially stale state map if token is missing
            state_map = {}
            if state_file.exists():
                 log.warning("Deleting old state map %s before full sync (token missing).", state_file)
                 try: state_file.unlink()
                 except OSError as e: log.error("Failed to delete old state map: %s", e)
        else:
             log.info(f"Found start token for {drive_name}. Proceeding with incremental sync.")
    else:
        log.warning(f"Incremental flag not set for {drive_name}. Performing full sync.")
        needs_full_sync = True
        # Clear state and token files for a clean full sync
        state_map = {}
        if state_file.exists():
             log.warning("Deleting old state map %s before forced full sync.", state_file)
             try: state_file.unlink()
             except OSError as e: log.error("Failed to delete old state map: %s", e)
        if token_file.exists():
            log.warning("Deleting old start token %s before forced full sync.", token_file)
            try: token_file.unlink(missing_ok=True)
            except OSError as e: log.error("Failed to delete old start token file: %s", e)

    # --- Perform Sync ---    
    if needs_full_sync:
        # Full sync mode
        log.info(f"Starting full sync for {drive_name} using files.list")
        try:
            # Perform the full sync which populates the state_map
            processed, downloaded, deleted, failed, shortcuts_skipped = perform_full_sync(
                drive_service=drive_service,
                gspread_client=gspread_client,
                drive_id=drive_id,
                drive_name=drive_name,
                drive_backup_dir=drive_backup_dir,
                state_map=state_map, # Pass the map to be populated
                processed_shared_drive_ids=processed_shared_drive_ids,
                dry_run=dry_run
            )
            
            processed_count += processed
            downloaded_count += downloaded
            # deleted_count is 0 from perform_full_sync
            failed_count += failed
            shortcuts_skipped_count += shortcuts_skipped
            
            # After successful full sync, get the initial start token for the *next* run
            # Calculate success rate - save token if 98%+ successful (excluding Google Shortcuts from calculation)
            effective_processed = processed_count  # Total processed files (including shortcuts that were attempted)
            critical_failures = failed_count  # All failures are considered for now
            
            success_rate = (effective_processed - critical_failures) / effective_processed if effective_processed > 0 else 0
            success_percentage = success_rate * 100
            
            if shortcuts_skipped_count > 0:
                log.info(f"Full sync for {drive_name}: {shortcuts_skipped_count} Google Shortcuts were skipped (not counted as failures)")
            
            if success_rate >= 0.98: # Save token if 98%+ successful
                log.info(f"Full sync for {drive_name} achieved {success_percentage:.1f}% success rate ({effective_processed - critical_failures}/{effective_processed}). Saving token.")
                new_start_token = state_manager.get_initial_start_page_token(drive_service, drive_id)
                if new_start_token:
                    if not dry_run:
                        # Save the populated state map and the new start token
                        state_manager.save_drive_state(state_map, state_file) 
                        state_manager.save_start_page_token(new_start_token, token_file)
                        log.info(f"Full sync for {drive_name} completed. State and new token saved.")
                    else:
                        # Save state map (potentially with size info if perform_full_sync added it), but not token
                        # Note: Current perform_full_sync doesn't calculate size, but save_drive_state handles it if passed.
                        state_manager.save_drive_state(state_map, state_file, total_size_bytes=None)
                        log.info(f"Full sync (DRY RUN) for {drive_name} completed. State map saved, token not saved.")
                else:
                     log.error(f"Failed to get initial start token after full sync for {drive_name}. Next run will require another full sync.")
                     # Still save the state we got, even if the token failed
                     state_manager.save_drive_state(state_map, state_file)
                     failed_count += 1 # Add a failure for the token fetch
            else:
                log.warning(f"Full sync for {drive_name} had {success_percentage:.1f}% success rate ({processed_count - failed_count}/{processed_count}). Token not saved due to low success rate (<98%).")
                # Save potentially incomplete state anyway for debugging?
                state_manager.save_drive_state(state_map, state_file)
                
        except HttpError as e:
            # Handle errors during the full sync process itself (e.g., auth errors)
            if e.resp.status == 401:
                log.error(f"Authorization error during full sync for {drive_name}. Please re-authenticate.")
                raise # Re-raise to stop the main loop
            else:
                log.error(f"API error during full sync for {drive_name}: {e}")
                failed_count += 1
        except Exception as e:
            log.error(f"Error during full sync for {drive_name}: {e}", exc_info=True)
            failed_count += 1
            
    else: # Incremental sync mode (token was loaded successfully)
        log.info(f"Starting incremental sync for {drive_name} from token: {start_token[:10]}...")
        try:
            # Use process_changes for incremental sync
            processed, downloaded, deleted, failed = process_changes(
                drive_service=drive_service,
                gspread_client=gspread_client,
                drive_id=drive_id,
                drive_name=drive_name,
                drive_backup_dir=drive_backup_dir,
                state_map=state_map, # Pass the loaded state map
                start_token=start_token, # Pass the loaded token
                processed_shared_drive_ids=processed_shared_drive_ids,
                dry_run=dry_run
            )
            
            processed_count += processed
            downloaded_count += downloaded
            deleted_count += deleted
            failed_count += failed
            
            # process_changes should update the state_map directly
            # It also handles getting and saving the *new* start token internally if not dry_run
            # So, we just need to save the final state map after the loop finishes
            if not dry_run:
                state_manager.save_drive_state(state_map, state_file)
                log.info(f"Incremental sync for {drive_name} finished. Final state saved.")
                # Token saving is handled within process_changes loop
            else:
                 state_manager.save_drive_state(state_map, state_file)
                 log.info(f"Incremental sync (DRY RUN) for {drive_name} finished. State saved, token not saved.")

        except HttpError as e:
            if e.resp.status == 401:
                log.error(f"Authorization error during incremental sync for {drive_name}. Please re-authenticate.")
                raise # Re-raise to stop main loop
            elif e.resp.status == 404 and "pageToken not found" in str(e):
                log.error(f"Invalid page token during incremental sync for {drive_name}. Full sync needed on next run.")
                # State saving is skipped here, as the sync failed mid-way
                failed_count += 1
                # Don't re-raise here? Allow script to continue with other drives, but log the failure.
            else:
                log.error(f"API error during incremental sync for {drive_name}: {e}")
                failed_count += 1
                # Save state map as it is after the error?
                state_manager.save_drive_state(state_map, state_file)
        except Exception as e:
            log.error(f"Error during incremental sync for {drive_name}: {e}", exc_info=True)
            failed_count += 1
            # Save state map as it is after the error?
            state_manager.save_drive_state(state_map, state_file)
    
    actual_mode = "full" if needs_full_sync else "incremental"
    log.info(f"--- Finished processing for drive: {drive_name} --- Counts: Processed={processed_count}, Downloaded={downloaded_count}, Deleted={deleted_count}, Failed={failed_count}")
    return processed_count, downloaded_count, deleted_count, failed_count, actual_mode

def process_changes(
    drive_service: Resource,
    gspread_client: Optional[gspread.Client],
    drive_id: Optional[str],
    drive_name: str,
    drive_backup_dir: Path,
    state_map: Dict[str, Dict[str, Any]],
    start_token: str,
    processed_shared_drive_ids: Set[str],
    dry_run: bool
) -> Tuple[int, int, int, int]:
    """
    Process changes from the Drive API.
    Returns (processed_count, downloaded_count, deleted_count, failed_count).
    """
    processed_count = 0
    downloaded_count = 0
    deleted_count = 0
    failed_count = 0
    
    page_token = start_token
    while page_token:
        try:
            # Get changes
            changes_params = {
                "pageToken": page_token,
                "spaces": "drive",
                "fields": "nextPageToken, newStartPageToken, changes(fileId, time, file(id, name, mimeType, size, parents, trashed))",
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True
            }
            if drive_id:
                changes_params["driveId"] = drive_id
                
            changes_result = drive_service.changes().list(**changes_params).execute()
            changes = changes_result.get("changes", [])
            
            # Process each change
            for change in changes:
                processed_count += 1
                
                # Get file details
                file_details = change.get("file", {})
                if not file_details:
                    continue
                    
                file_id = file_details.get("id")
                if not file_id:
                    continue
                    
                # Skip if file is in a shared drive we've already processed
                if drive_id and drive_id in processed_shared_drive_ids:
                    continue
                    
                        # --- Skip Shared Drive files when processing 'My Drive' incrementally ---
                is_my_drive_processing = drive_id is None
                shared_drive_id = file_details.get("driveId") if file_details else None # Get driveId from file_details if available

                # We only apply this logic if the change is NOT a deletion and we are in My Drive sync
                if is_my_drive_processing and shared_drive_id and not file_details.get("trashed", False):
                    if shared_drive_id in processed_shared_drive_ids:
                        # Skip logging for each skipped file to reduce log spam
                        continue # Skip processing this change
                    else:
                        # Handle change for item belonging to a shared drive NOT processed separately
                        item_name = file_details.get("name", "_unnamed_")
                        log.warning(f"Change for item '{item_name}' ({file_id}) found during 'My Drive' sync belongs to Shared Drive {shared_drive_id} (NOT processed separately). Processing in '{config.SHARED_FILES_DIR_NAME}'.")
                        target_dir = config.SHARED_FILES_DIR / shared_drive_id
                        target_dir.mkdir(parents=True, exist_ok=True)
                        target_path_base = target_dir / utils.sanitize_filename(item_name)
                        mime_type = file_details.get("mimeType")

                        # processed_count was already incremented at the start of the loop
                        if mime_type == config.FOLDER_MIME_TYPE:
                            try:
                                target_path_base.mkdir(parents=True, exist_ok=True)
                                downloaded_count += 1
                            except OSError as e:
                                log.error(f"Failed to create folder in Shared With Me dir: {target_path_base} - {e}")
                                failed_count += 1
                        elif mime_type:
                            # Download/update without adding to state map or S3
                            success, _ = file_processor.download_file(
                                service=drive_service,
                                item=file_details, # Use file_details from the change
                                local_path_base=target_path_base,
                                gspread_client=gspread_client
                            )
                            if success:
                                downloaded_count += 1
                            else:
                                failed_count += 1
                        else:
                            log.warning(f"Item '{item_name}' in Shared Drive {shared_drive_id} has no mimeType. Skipping change.")
                            failed_count += 1
                        continue # Skip normal processing and state map update
                #elif is_my_drive_processing and is_removed and file_id in some_way_to_track_shared_files:
                    # Handle deletion of a shared file - currently not implemented easily
                    # log.info(f"Deletion detected for item {file_id} potentially in Shared With Me. Manual cleanup may be needed.")

                # Handle file changes
                try:
                    if file_details.get("trashed", False):
                        # File was deleted
                        if file_id in state_map:
                            deleted_count += 1
                            del state_map[file_id]
                            log.info(f"Deleted file: {file_details.get('name', file_id)}")
                    else:
                        # File was modified or created
                        file_name = file_details.get("name", "_unnamed_")
                        mime_type = file_details.get("mimeType", "")
                        
                        # Skip folders in dry run
                        if dry_run and mime_type == config.FOLDER_MIME_TYPE:
                            continue
                            
                        # Get or create local path
                        local_path = file_processor.reconstruct_and_create_path(
                            service=drive_service,
                            item_id=file_id,
                            item_name=file_name,
                            item_parents=file_details.get("parents", []),
                            drive_id=drive_id,
                            drive_backup_dir=drive_backup_dir
                        )
                        
                        if not local_path:
                            log.error(f"Failed to get local path for {file_name}")
                            failed_count += 1
                            continue
                            
                        # Download file
                        success, final_path = file_processor.download_file(
                            service=drive_service,
                            item=file_details,
                            local_path_base=local_path,
                            gspread_client=gspread_client
                        )
                        
                        if success:
                            downloaded_count += 1
                            # Update state map
                            state_map[file_id] = {
                                "path": str(final_path.relative_to(drive_backup_dir)),
                                "modifiedTime": change.get("time"),
                                "is_folder": mime_type == config.FOLDER_MIME_TYPE
                            }
                            # Reduce logging frequency - only log every 100th file or important files
                            if processed_count % 100 == 0 or mime_type in config.GOOGLE_DOCS_MIMETYPES:
                                log.info(f"Downloaded/updated: {file_name} (processed {processed_count} items)")
                        else:
                            failed_count += 1
                            log.error(f"Failed to download/update: {file_name}")
                            
                except HttpError as e:
                    if e.resp.status == 404:
                        log.warning(f"File not found (404): {file_details.get('name', file_id)}")
                        if file_id in state_map:
                            deleted_count += 1
                            del state_map[file_id]
                    else:
                        log.error(f"API error processing file {file_details.get('name', file_id)}: {e}")
                        failed_count += 1
                except Exception as e:
                    log.error(f"Error processing file {file_details.get('name', file_id)}: {e}", exc_info=True)
                    failed_count += 1
                    
            # Get next page token
            page_token = changes_result.get("nextPageToken")
            
            # Update start token for next run
            if "newStartPageToken" in changes_result:
                start_token = changes_result["newStartPageToken"]
                
        except HttpError as e:
            if e.resp.status == 401:
                log.error(f"Authorization error. Please re-authenticate.")
                raise
            elif e.resp.status == 404 and "pageToken not found" in str(e):
                log.error(f"Invalid page token. Full sync needed.")
                raise
            else:
                log.error(f"API error: {e}")
                failed_count += 1
                break
        except Exception as e:
            log.error(f"Error processing changes: {e}", exc_info=True)
            failed_count += 1
            break
            
    return processed_count, downloaded_count, deleted_count, failed_count

# --- Full Sync Function ---
def perform_full_sync(
    drive_service: Resource,
    gspread_client: Optional[gspread.Client],
    drive_id: Optional[str],
    drive_name: str,
    drive_backup_dir: Path,
    state_map: Dict[str, Dict[str, Any]], # Pass the state map to populate it
    processed_shared_drive_ids: Set[str], # To skip shared drive items during My Drive sync
    dry_run: bool = False,
    max_retries: int = 10  # Increased from 3 to 10 for SSL stability
) -> Tuple[int, int, int, int, int]: # Returns processed, downloaded, deleted, failed, shortcuts_skipped counts
    """
    Performs a full sync by listing all files using files.list.
    Populates the state_map.
    Returns counts: (processed, downloaded, deleted, failed).
    """
    log.info(f"Performing full sync for drive '{drive_name}' using files.list... {'(DRY RUN)' if dry_run else ''}")
    processed_count = 0
    downloaded_count = 0
    deleted_count = 0 # Not applicable in full sync from scratch
    failed_count = 0
    shortcuts_skipped_count = 0
    all_items_map = {} # Temporary map to hold all fetched items {id: item}

    # --- 1. Fetch all items using files.list ---
    try:
        page_token = None
        log.info(f"Fetching full list of objects for drive: '{drive_name}'")
        # Base query parameters
        list_params = {
            "pageSize": 1000,
            "q": "trashed = false",
            "fields": "nextPageToken, files(id, name, parents, mimeType, modifiedTime, size, driveId)", # Removed shortcutDetails for now
            "orderBy": "folder, name",
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True
        }
        if drive_id:
            list_params["driveId"] = drive_id
            list_params["corpora"] = "drive"
        else:
            list_params["corpora"] = "user" # For My Drive

        # Fetch loop with retry logic
        while True:
            if page_token: list_params["pageToken"] = page_token
            else: list_params.pop("pageToken", None)

            # Retry logic for API calls with rate limiting
            limiter = rate_limiter.get_rate_limiter()
            for retry_attempt in range(max_retries):
                try:
                    if retry_attempt > 0:
                        log.info(f"🔄 Retry attempt {retry_attempt + 1}/{max_retries} for drive '{drive_name}' API call")
                    
                    # Use rate limiter to prevent overwhelming the API
                    with limiter:
                        results = drive_service.files().list(**list_params).execute()
                    
                    items = results.get("files", [])
                    if retry_attempt > 0:
                        log.info(f"✅ API call succeeded on attempt {retry_attempt + 1} for drive '{drive_name}'")
                    break  # Success, exit retry loop
                except ssl.SSLError as e:
                    # Report SSL error to rate limiter for adaptive throttling
                    limiter.report_ssl_error()
                    
                    if retry_attempt < max_retries - 1:
                        # Exponential backoff with longer delays for SSL issues
                        base_delay = min(30, (3 ** retry_attempt))  # Cap at 30 seconds
                        jitter = random.uniform(0, 5)  # Add more jitter
                        wait_time = base_delay + jitter
                        log.warning(f"SSL error during API call for '{drive_name}' (attempt {retry_attempt + 1}/{max_retries}): {e}. Retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                        log.error(f"SSL error during API call for '{drive_name}' after {max_retries} attempts: {e}")
                        raise
                except HttpError as e:
                    if retry_attempt < max_retries - 1 and e.resp.status >= 500:
                        wait_time = (2 ** retry_attempt) + random.uniform(0, 1)
                        log.warning(f"Server error {e.resp.status} during API call for '{drive_name}' (attempt {retry_attempt + 1}/{max_retries}): {e}. Retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                        raise

            for item in items:
                # Skip shortcuts (though field wasn't requested, good practice)
                if item.get("mimeType") == "application/vnd.google-apps.shortcut":
                    shortcuts_skipped_count += 1
                    continue

                # --- Filter out Shared Drive items when processing 'My Drive' ---
                is_my_drive_processing = drive_id is None
                item_belongs_to_shared_drive_id = item.get("driveId")
                if is_my_drive_processing and item_belongs_to_shared_drive_id:
                    if item_belongs_to_shared_drive_id in processed_shared_drive_ids:
                         # Skip debug logging to reduce spam
                         continue # Skip this item, it belongs to a drive processed elsewhere
                    else:
                         # Handle item belonging to a shared drive NOT processed separately
                         item_name = item.get("name", "_unnamed_")
                         # Only log first few items to avoid spam
                         if processed_count < 5:
                             log.warning(f"Item '{item_name}' ({item['id']}) found during 'My Drive' sync belongs to Shared Drive {item_belongs_to_shared_drive_id} (NOT processed separately). Downloading to '{config.SHARED_FILES_DIR_NAME}'.")
                         target_dir = config.SHARED_FILES_DIR / item_belongs_to_shared_drive_id # Subfolder per drive ID
                         target_dir.mkdir(parents=True, exist_ok=True)
                         target_path_base = target_dir / utils.sanitize_filename(item_name)
                         mime_type = item.get("mimeType")

                         processed_count += 1 # Count as processed
                         if mime_type == config.FOLDER_MIME_TYPE:
                             try:
                                 target_path_base.mkdir(parents=True, exist_ok=True)
                                 downloaded_count += 1 # Count folder creation
                             except OSError as e:
                                 log.error(f"Failed to create folder in Shared With Me dir: {target_path_base} - {e}")
                                 failed_count += 1
                         elif mime_type:
                             # Download without adding to state map or uploading to S3
                             success, _ = file_processor.download_file(
                                 service=drive_service,
                                 item=item,
                                 local_path_base=target_path_base,
                                 gspread_client=gspread_client
                             )
                             if success:
                                 downloaded_count += 1
                             else:
                                 failed_count += 1
                         else:
                             log.warning(f"Item '{item_name}' in Shared Drive {item_belongs_to_shared_drive_id} has no mimeType. Skipping.")
                             failed_count += 1
                         continue # Skip normal processing and state map update

                # Store valid items for normal processing
                all_items_map[item["id"]] = item

            page_token = results.get("nextPageToken")
            if not page_token: break
        log.info(f"Found {len(all_items_map)} total objects for full sync on '{drive_name}'.")

    except HttpError as error:
        log.error(f"API error during full scan of '{drive_name}': {error}. Full sync aborted.", exc_info=True)
        return processed_count, downloaded_count, deleted_count, 1, shortcuts_skipped_count # Return 1 failure
    except ssl.SSLError as e:
        log.error(f"SSL connection error during full scan of '{drive_name}': {e}. Full sync aborted.")
        return processed_count, downloaded_count, deleted_count, 1, shortcuts_skipped_count # Return 1 failure
    except Exception as e:
        log.error(f"Unknown error during full scan of '{drive_name}': {e}. Full sync aborted.", exc_info=True)
        return processed_count, downloaded_count, deleted_count, 1, shortcuts_skipped_count # Return 1 failure

    # --- 1.5 Item Sampling for Dry Run ---
    items_to_process_list = list(all_items_map.values())
    if dry_run:
        # Separate folders and files meeting size criteria
        folders = [item for item in items_to_process_list if item.get("mimeType") == config.FOLDER_MIME_TYPE]
        small_files = [
            item for item in items_to_process_list
            if item.get("mimeType") != config.FOLDER_MIME_TYPE and int(item.get("size", 0)) <= config.DRY_RUN_MAX_FILE_SIZE_BYTES
        ]
        # Sort small files by size (optional, but helps select smallest first)
        small_files.sort(key=lambda x: int(x.get("size", 0)))
        
        # Build the sampled list
        sampled_items = []
        # Sample some folders (e.g., up to half the sample size)
        sampled_items.extend(random.sample(folders, min(len(folders), config.DRY_RUN_SAMPLE_SIZE // 2)))
        # Fill remaining sample slots with the smallest files
        remaining_sample_size = config.DRY_RUN_SAMPLE_SIZE - len(sampled_items)
        if remaining_sample_size > 0:
            sampled_items.extend(small_files[:min(len(small_files), remaining_sample_size)])
        
        # Handle edge case: if no folders/small files, sample randomly from all items
        if not sampled_items and items_to_process_list:
            log.warning("[DRY RUN] No folders or small files found for sampling. Sampling randomly from all items.")
            sampled_items = random.sample(items_to_process_list, min(len(items_to_process_list), config.DRY_RUN_SAMPLE_SIZE))
            
        items_to_process_list = sampled_items # Replace the list with the sampled items
        log.info(f"[DRY RUN] Selected {len(items_to_process_list)} items for processing based on sampling rules.")

    # --- 2. Process all fetched (or sampled) items ---
    log.info(f"Processing {len(items_to_process_list)} items for full sync on '{drive_name}'...")
    
    # Progress reporting
    total_items = len(items_to_process_list)
    last_progress_report = 0

    for item in items_to_process_list:
        processed_count += 1
        item_id = item["id"]
        item_name = item.get("name", "_unnamed_")
        mime_type = item.get("mimeType")
        is_folder = mime_type == config.FOLDER_MIME_TYPE
        
        # Report progress every 10% or every 500 items
        progress_percentage = (processed_count * 100) // total_items
        if progress_percentage >= last_progress_report + 10 or processed_count % 500 == 0:
            log.info(f"Full sync progress: {processed_count}/{total_items} ({progress_percentage}%) - Current: {item_name[:50]}...")
            last_progress_report = progress_percentage

        try:
            # Get local path using the reconstructor
            local_path_base = file_processor.reconstruct_and_create_path(
                service=drive_service,
                item_id=item_id,
                item_name=item_name,
                item_parents=item.get("parents"),
                drive_id=drive_id, # Pass the context drive_id
                drive_backup_dir=drive_backup_dir
            )

            if not local_path_base:
                log.error(f"Full Sync: Failed to get local path for {item_name} ({item_id}). Skipping.")
                failed_count += 1
                continue

            if is_folder:
                # Ensure the folder exists locally (reconstruct_and_create_path might create parents, but not the final one)
                if not local_path_base.exists():
                    local_path_base.mkdir(parents=True, exist_ok=True)
                elif not local_path_base.is_dir():
                     log.error(f"Full Sync: Path for folder {item_name} exists but is not a directory: {local_path_base}. Skipping.")
                     failed_count += 1
                     continue
                # Update state map for folder
                state_map[item_id] = {
                    "path": str(local_path_base.relative_to(drive_backup_dir)),
                    "modifiedTime": item.get("modifiedTime"),
                    "is_folder": True
                }
                downloaded_count += 1 # Count folder creation as "downloaded" activity
                # No S3 action for folders

            else: # It's a file
                # Download/Export the file (includes potential S3 upload)
                # download_file handles adding the extension
                success, final_local_path = file_processor.download_file(
                    service=drive_service,
                    item=item,
                    local_path_base=local_path_base, # Pass the path without extension initially
                    gspread_client=gspread_client
                )

                if success:
                    downloaded_count += 1
                    # Update state map for file using the final path
                    state_map[item_id] = {
                        "path": str(final_local_path.relative_to(drive_backup_dir)),
                        "modifiedTime": item.get("modifiedTime"),
                        "is_folder": False
                    }
                else:
                    failed_count += 1
                    log.error(f"Full Sync: Failed to download/export file {item_name} ({item_id})")

        except Exception as e:
            log.error(f"Full Sync: Error processing item {item_name} ({item_id}): {e}", exc_info=True)
            failed_count += 1

    log.info(f"Full sync processing for '{drive_name}' finished.")
    return processed_count, downloaded_count, deleted_count, failed_count, shortcuts_skipped_count
