# -*- coding: utf-8 -*-
import argparse
import logging
import time
from pathlib import Path
from typing import Optional, Set, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from googleapiclient.discovery import Resource
import gspread

from src import config
from src import google_api
from src import sync
from src import archive
from src import utils
from src import s3
from src import rate_limiter
from src.logger import driveup_logger

# Initialize logger using the config setup
log = logging.getLogger(__name__)

def check_disk_space(required_gb: float = 10.0) -> bool:
    """
    Check if there's enough free disk space for backup operations.
    Returns True if enough space, False otherwise.
    """
    import shutil
    
    try:
        # Check free space in the archive directory
        archive_dir = Path(config.ARCHIVE_DIR)
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        total, used, free = shutil.disk_usage(archive_dir)
        free_gb = free / (1024**3)
        
        log.info(f"Disk space check: {free_gb:.1f}GB free (required: {required_gb:.1f}GB)")
        
        if free_gb < required_gb:
            log.error(f"❌ Insufficient disk space! Only {free_gb:.1f}GB free, need {required_gb:.1f}GB")
            return False
        else:
            log.info(f"✅ Sufficient disk space: {free_gb:.1f}GB available")
            return True
            
    except Exception as e:
        log.error(f"Failed to check disk space: {e}")
        return False

def process_single_drive(
    creds: Any,  # Use Any to avoid circular import with google.oauth2.credentials
    drive: dict,
    processed_drive_ids: Set[str],
    incremental_flag: bool,
    dry_run: bool
) -> tuple[int, int, int, int, str]:
    """
    Process a single shared drive safely in a separate thread.
    Creates its own thread-safe API clients.
    """
    try:
        drive_id = drive['id']
        drive_name = drive['name']
        
        # Create new, thread-safe clients for this worker
        drive_service, gspread_client = google_api.create_service_clients_from_creds(creds)
        
        log.info(f"🔄 Starting parallel processing of drive: {drive_name}")
        
        # Create drive-specific directories
        safe_drive_name = utils.sanitize_filename(drive_name)
        drive_backup_dir = config.BASE_DOWNLOAD_DIR / safe_drive_name
        drive_state_dir = config.STATE_DIR / safe_drive_name
        
        # Create directories if they don't exist
        drive_backup_dir.mkdir(parents=True, exist_ok=True)
        drive_state_dir.mkdir(parents=True, exist_ok=True)
        
        # Process the drive
        processed, downloaded, deleted, failed, actual_mode = sync.process_drive(
            drive_service=drive_service,
            gspread_client=gspread_client,
            drive_id=drive_id,
            drive_name=drive_name,
            drive_backup_dir=drive_backup_dir,
            drive_state_dir=drive_state_dir,
            processed_shared_drive_ids=processed_drive_ids,
            incremental_flag=incremental_flag,
            dry_run=dry_run
        )
        
        log.info(f"✅ Completed parallel processing of drive: {drive_name} - P:{processed}/D:{downloaded}/Del:{deleted}/F:{failed} (Mode: {actual_mode})")
        return processed, downloaded, deleted, failed, drive_name
        
    except Exception as e:
        log.error(f"❌ Error processing drive {drive.get('name', 'Unknown')}: {e}", exc_info=True)
        return 0, 0, 0, 1, drive.get('name', 'Unknown')  # Return 1 failure

# --- Try importing Boto3 for S3 (this is now handled in s3.py, but keep for dummy exceptions) ---
if config.BOTO3_AVAILABLE:
    import boto3 # boto3 import itself is fine here
    from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
else:
    NoCredentialsError = type('NoCredentialsError', (Exception,), {})
    PartialCredentialsError = type('PartialCredentialsError', (Exception,), {})
    ClientError = type('ClientError', (Exception,), {})

def process_shared_drives(
    creds: Any,
    incremental_flag: bool,
    dry_run: bool,
    max_workers: int = 1
) -> tuple[int, int, int, int, Set[str]]:
    """
    Process all shared drives.
    Returns (processed_count, downloaded_count, deleted_count, failed_count, processed_drive_ids).
    """
    processed_count = 0
    downloaded_count = 0
    deleted_count = 0
    failed_count = 0
    processed_drive_ids: Set[str] = set()
    
    try:
        # Create a temporary service client just for listing drives
        drive_service_main, _ = google_api.create_service_clients_from_creds(creds)
        
        # List all shared drives
        drives_result = drive_service_main.drives().list(
            pageSize=100,
            fields="drives(id, name)"
        ).execute()
        
        drives = drives_result.get('drives', [])
        log.info(f"Found {len(drives)} shared drives")
        
        if max_workers == 1:
            # Sequential processing (safe default)
            log.info("🔄 Using sequential processing (max_workers=1)")
            for drive in drives:
                processed, downloaded, deleted, failed, drive_name = process_single_drive(
                    creds, drive, processed_drive_ids, incremental_flag, dry_run
                )
                processed_count += processed
                downloaded_count += downloaded
                deleted_count += deleted
                failed_count += failed
                processed_drive_ids.add(drive['id'])
        else:
            # Parallel processing
            log.info(f"🚀 Using parallel processing with {max_workers} workers")
            log.warning("⚠️  PARALLEL MODE: Ensure sufficient system resources and API quotas!")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all drive processing tasks
                future_to_drive = {
                    executor.submit(
                        process_single_drive, 
                        creds, drive, processed_drive_ids, incremental_flag, dry_run
                    ): drive for drive in drives
                }
                
                # Collect results as they complete
                ssl_error_count = 0
                for future in as_completed(future_to_drive):
                    drive = future_to_drive[future]
                    try:
                        processed, downloaded, deleted, failed, drive_name = future.result()
                        processed_count += processed
                        downloaded_count += downloaded
                        deleted_count += deleted
                        failed_count += failed
                        processed_drive_ids.add(drive['id'])
                        
                        # Check for SSL-related failures
                        if failed > 0 and processed == 0:
                            ssl_error_count += 1
                            log.warning(f"🔥 Drive '{drive_name}' appears to have SSL/network issues (P:0/F:{failed})")
                        
                        log.info(f"📊 Drive '{drive_name}' completed: P:{processed}/D:{downloaded}/Del:{deleted}/F:{failed}")
                    except Exception as e:
                        log.error(f"❌ Drive '{drive.get('name', 'Unknown')}' failed: {e}", exc_info=True)
                        failed_count += 1
                        ssl_error_count += 1
                
                # Check for critical failures
                total_drives = len(drives)
                successful_drives = total_drives - ssl_error_count
                
                # Critical failure detection
                if ssl_error_count == total_drives and ssl_error_count > 0:
                    log.error(f"🚨 CRITICAL: All {ssl_error_count} drives failed with SSL/network errors!")
                    log.error("🚨 This indicates a systemic network connectivity issue.")
                    log.error("🚨 Backup job should be considered FAILED despite GitLab success status.")
                    return 0, 0, 0, total_drives, processed_drive_ids  # Return failure counts
                
                # Check if we have significantly fewer files than expected (based on historical data)
                expected_minimum_files = 20000  # Based on logs22/23 having ~25k files
                if processed_count < expected_minimum_files and ssl_error_count > 0:
                    log.error(f"🚨 CRITICAL: Only {processed_count} files processed (expected >{expected_minimum_files})")
                    log.error(f"🚨 {ssl_error_count}/{total_drives} drives failed with SSL errors")
                    log.error(f"🚨 This represents a {((expected_minimum_files - processed_count) / expected_minimum_files * 100):.1f}% data loss!")
                    log.error("🚨 Backup should be considered INCOMPLETE and FAILED")
                    return processed_count, downloaded_count, deleted_count, failed_count + ssl_error_count, processed_drive_ids
            
    except Exception as e:
        log.error(f"Error processing shared drives: {e}", exc_info=True)
        
    return processed_count, downloaded_count, deleted_count, failed_count, processed_drive_ids

def main():
    parser = argparse.ArgumentParser(
        description="Backup Google Drive to local storage and optionally to S3. Choose ONE sync mode.",
    )
    mode_group = parser.add_mutually_exclusive_group(required=True) 
    mode_group.add_argument("--full", action="store_true", help="Perform a full backup (download all)")
    mode_group.add_argument("--incremental", action="store_true", help="Perform an incremental backup (only changes)")
    
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without making changes or saving tokens")
    parser.add_argument("--s3-bucket", help="S3 bucket name for backup storage")
    parser.add_argument("--s3-prefix", help="Prefix (folder path) within the S3 bucket")
    parser.add_argument("--s3-endpoint", help="S3 endpoint URL for non-AWS storage")
    parser.add_argument("--s3-region", help="S3 region name")
    parser.add_argument("--s3-access-key", help="S3 access key ID")
    parser.add_argument("--s3-secret-key", help="S3 secret access key")
    parser.add_argument("--no-archive", action="store_true", help="Skip creating archive after backup")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                      help="Set the logging level")
    parser.add_argument("--max-workers", type=int, default=1, 
                      help="Maximum number of parallel workers for drive processing (default: 1 for safety)")
    
    args = parser.parse_args()
    
    # Initialize our custom logger
    driveup_logger.setup(log_level=args.log_level)
    
    # Check disk space before starting backup
    if not check_disk_space(required_gb=15.0):  # Require 15GB free space
        log.error("Backup aborted due to insufficient disk space")
        return 1
    
    # Initialize S3 client using the new s3 module
    s3_client, s3_enabled = s3.setup_s3_client(
        args.s3_bucket,
        s3_endpoint_url=args.s3_endpoint,
        s3_region=args.s3_region,
        s3_access_key=args.s3_access_key,
        s3_secret_key=args.s3_secret_key
    )

    try:
        # Get Google API credentials
        creds = google_api.get_credentials()
        if not creds:
            log.error("Failed to get Google API credentials")
            return 1
            
        # Create base directories
        config.BASE_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        config.STATE_DIR.mkdir(parents=True, exist_ok=True)
        if not args.no_archive:
            config.ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        
        # Initialize rate limiter for parallel processing
        if args.max_workers > 1:
            limiter = rate_limiter.init_rate_limiter(
                max_workers=args.max_workers,
                min_delay=0.3  # 300ms minimum delay between API calls
            )
            log.info(f"⏱️  Rate limiting enabled: {args.max_workers} workers, adaptive throttling active")
        else:
            # Single-threaded mode with minimal delay
            limiter = rate_limiter.init_rate_limiter(max_workers=1, min_delay=0.05)
            
        # Process shared drives first
        shared_processed, shared_downloaded, shared_deleted, shared_failed, processed_drive_ids = process_shared_drives(
            creds=creds,
            incremental_flag=args.incremental,
            dry_run=args.dry_run,
            max_workers=args.max_workers
        )
        shared_modes = ["full"] * shared_processed  # Assume full mode for now
        
        # Create a service client for the main thread (My Drive processing)
        drive_service, gspread_client = google_api.create_service_clients_from_creds(creds)
        
        # Process My Drive
        my_drive_processed, my_drive_downloaded, my_drive_deleted, my_drive_failed, my_drive_mode = sync.process_drive(
            drive_service=drive_service,
            gspread_client=gspread_client,
            drive_id=None,  
            drive_name="My Drive",
            drive_backup_dir=config.BASE_DOWNLOAD_DIR / "My Drive",
            drive_state_dir=config.STATE_DIR / "My Drive",
            processed_shared_drive_ids=processed_drive_ids,
            incremental_flag=args.incremental,
            dry_run=args.dry_run
        )
        # Calculate totals
        total_processed = shared_processed + my_drive_processed
        total_downloaded = shared_downloaded + my_drive_downloaded
        total_deleted = shared_deleted + my_drive_deleted
        total_failed = shared_failed + my_drive_failed
        # Determine final archive mode
        all_modes = shared_modes + [my_drive_mode]
        archive_mode = "full" if "full" in all_modes else "incremental"
        # Create archive if requested and there were changes (or always in dry-run for S3 testing)
        should_create_archive = not args.no_archive and (total_downloaded > 0 or total_deleted > 0 or args.dry_run)
        if should_create_archive:
            # Double-check disk space before creating archive
            if not check_disk_space(required_gb=10.0):
                log.error("Skipping archive creation due to insufficient disk space")
                should_create_archive = False
        
        if should_create_archive:
            archive_created, archive_path, archive_name = archive.create_backup_archive(
                backup_dir=config.BASE_DOWNLOAD_DIR,
                dry_run=args.dry_run,
                mode=archive_mode
            )
            if archive_created and archive_path and archive_name:
                if args.dry_run:
                    log.info(f"Test archive created for S3 verification: {archive_path}")
                else:
                    log.info(f"Backup archived locally to: {archive_path}")
                
                if s3_enabled and s3_client:
                    s3_upload_success = s3.upload_archive_to_s3(
                        archive_path=str(archive_path),
                        s3_client=s3_client,
                        s3_bucket=args.s3_bucket,
                        s3_prefix=args.s3_prefix,
                        archive_name=archive_name
                    )
                    if s3_upload_success:
                        if args.dry_run:
                            log.info("✅ S3 upload test SUCCESSFUL! S3 configuration is working correctly.")
                            # Clean up test archive after successful upload
                            try:
                                archive_path.unlink()
                                log.info(f"Test archive cleaned up: {archive_name}")
                            except Exception as e:
                                log.warning(f"Failed to clean up test archive: {e}")
                        else:
                            log.info("Archive uploaded to S3 successfully")
                            # Clean up all archives from local storage after successful S3 upload
                            try:
                                archive_path.unlink()
                                log.info(f"Current archive removed from local storage: {archive_name}")
                                # Remove all other files from archive directory
                                removed_count = 0
                                for archive_file in config.ARCHIVE_DIR.glob("*"):
                                    if archive_file.is_file():
                                        try:
                                            archive_file.unlink()
                                            removed_count += 1
                                            log.debug(f"Removed old archive: {archive_file.name}")
                                        except Exception as e:
                                            log.warning(f"Failed to remove {archive_file.name}: {e}")
                                if removed_count > 0:
                                    log.info(f"Cleaned up {removed_count} old archive(s) from local storage")
                            except Exception as e:
                                log.warning(f"Failed to clean up archives after S3 upload: {e}")
                    else:
                        if args.dry_run:
                            log.error("❌ S3 upload test FAILED! Check S3 configuration and credentials.")
                        else:
                            log.error("Failed to upload archive to S3. The archive remains available locally.")
                else:
                    log.warning("⚠️ S3 is not configured! Archive remains in local storage and should be uploaded manually.")
        # Print summary
        log.info("Backup completed:")
        log.info(f"  Total files processed: {total_processed}")
        log.info(f"  Total files downloaded: {total_downloaded}")
        log.info(f"  Total files deleted: {total_deleted}")
        log.info(f"  Total files failed: {total_failed}")
        
        # Critical failure check - exit with error code if backup is incomplete
        expected_minimum_files = 20000  # Based on historical data (logs22/23 had ~25k files)
        if total_processed < expected_minimum_files:
            log.error(f"🚨 BACKUP INCOMPLETE: Only {total_processed}/{expected_minimum_files} expected files processed")
            log.error(f"🚨 Data loss: {((expected_minimum_files - total_processed) / expected_minimum_files * 100):.1f}%")
            log.error("🚨 EXITING WITH ERROR CODE 1 - JOB SHOULD BE MARKED AS FAILED")
            driveup_logger.write_summary()
            return 1
        
        if total_failed > 10:  # Allow some tolerance for minor failures
            log.error(f"🚨 TOO MANY FAILURES: {total_failed} files failed")
            log.error("🚨 EXITING WITH ERROR CODE 1 - JOB SHOULD BE MARKED AS FAILED") 
            driveup_logger.write_summary()
            return 1
        
        # Print rate limiter statistics if parallel mode was used
        if args.max_workers > 1:
            limiter = rate_limiter.get_rate_limiter()
            stats = limiter.get_stats()
            log.info("⚙️  Rate Limiter Statistics:")
            log.info(f"  Total API calls: {stats['total_calls']}")
            log.info(f"  SSL errors: {stats['ssl_errors']}")
            log.info(f"  Error rate: {stats['error_rate']:.2f}%")
            log.info(f"  Final delay: {stats['current_delay']:.3f}s")
        
        # Write final summary to log file
        driveup_logger.write_summary()
        
    except Exception as e:
        log.error(f"Backup failed: {e}", exc_info=True)
        driveup_logger.write_summary()  # Write summary even if we fail
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main()) 