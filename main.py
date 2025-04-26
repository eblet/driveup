# -*- coding: utf-8 -*-

import argparse
import logging
import time
from pathlib import Path
from typing import Optional, Set, Any

from googleapiclient.discovery import Resource
import gspread

from src import config
from src import google_api
from src import sync
from src import archive
from src import utils

# Initialize logger using the config setup
log = logging.getLogger(__name__)

# --- Try importing Boto3 for S3 ---
if config.BOTO3_AVAILABLE:
    import boto3
    from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
else:
    # Define dummy exceptions if Boto3 not available, for cleaner except blocks later
    NoCredentialsError = type('NoCredentialsError', (Exception,), {})
    PartialCredentialsError = type('PartialCredentialsError', (Exception,), {})
    ClientError = type('ClientError', (Exception,), {})

def setup_s3_client(s3_bucket: Optional[str]) -> tuple[Optional[Any], bool]:
    """
    Initialize S3 client if bucket is specified.
    Returns (s3_client, s3_enabled).
    """
    if not s3_bucket:
        return None, False
        
    if not config.BOTO3_AVAILABLE:
        log.error("S3 upload requested but boto3 is not installed. Please install it with: pip install boto3")
        return None, False
        
    try:
        s3_client = boto3.client('s3')
        log.info(f"S3 client initialized for bucket: {s3_bucket}")
        return s3_client, True
    except Exception as e:
        log.error(f"Failed to initialize S3 client: {e}")
        return None, False

def process_shared_drives(
    drive_service: Resource,
    gspread_client: Optional[gspread.Client],
    s3_client: Optional[Any],
    s3_bucket: Optional[str],
    s3_prefix: Optional[str],
    incremental_flag: bool,
    dry_run: bool
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
        # List all shared drives
        drives_result = drive_service.drives().list(
            pageSize=100,
            fields="drives(id, name)"
        ).execute()
        
        drives = drives_result.get('drives', [])
        log.info(f"Found {len(drives)} shared drives")
        
        # Process each shared drive
        for drive in drives:
            drive_id = drive['id']
            drive_name = drive['name']
            
            # Create drive-specific directories
            safe_drive_name = utils.sanitize_filename(drive_name)
            drive_backup_dir = config.BASE_DOWNLOAD_DIR / safe_drive_name
            drive_state_dir = config.STATE_DIR / safe_drive_name
            
            # Create directories if they don't exist
            drive_backup_dir.mkdir(parents=True, exist_ok=True)
            drive_state_dir.mkdir(parents=True, exist_ok=True)
            
            # Process the drive
            processed, downloaded, deleted, failed = sync.process_drive(
                drive_service=drive_service,
                gspread_client=gspread_client,
                drive_id=drive_id,
                drive_name=drive_name,
                drive_backup_dir=drive_backup_dir,
                drive_state_dir=drive_state_dir,
                processed_shared_drive_ids=processed_drive_ids,
                incremental_flag=incremental_flag,
                dry_run=dry_run,
                s3_client=s3_client,
                s3_bucket=s3_bucket,
                s3_base_prefix=f"{s3_prefix.rstrip('/')}/{safe_drive_name}" if s3_prefix else safe_drive_name
            )
            
            processed_count += processed
            downloaded_count += downloaded
            deleted_count += deleted
            failed_count += failed
            processed_drive_ids.add(drive_id)
            
    except Exception as e:
        log.error(f"Error processing shared drives: {e}", exc_info=True)
        
    return processed_count, downloaded_count, deleted_count, failed_count, processed_drive_ids

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Backup Google Drive to local storage and optionally to S3. Choose ONE sync mode.",
        # Prevent default behavior when no arguments are given
        # usage='%(prog)s [--full | --incremental] [options]'
    )
    # Create a mutually exclusive group for sync modes
    mode_group = parser.add_mutually_exclusive_group(required=True) 
    mode_group.add_argument("--full", action="store_true", help="Perform a full backup (download all)")
    mode_group.add_argument("--incremental", action="store_true", help="Perform an incremental backup (only changes)")
    
    # Other optional arguments
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without making changes or saving tokens")
    parser.add_argument("--s3-bucket", help="S3 bucket name for backup storage")
    parser.add_argument("--s3-prefix", help="Prefix (folder path) within the S3 bucket")
    parser.add_argument("--no-archive", action="store_true", help="Skip creating archive after backup")
    
    args = parser.parse_args()
    
    # Determine incremental flag based on the chosen mode
    # No need for this check anymore, argparse handles mutual exclusion and requirement
    # incremental_sync_flag = args.incremental
    # If not args.full and not args.incremental:
    #     parser.print_help()
    #     log.error("Error: Please specify either --full or --incremental sync mode.")
    #     return 1
        
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize S3 if requested
    s3_client, s3_enabled = setup_s3_client(args.s3_bucket)
    
    try:
        # Get Google API credentials
        creds = google_api.get_credentials()
        if not creds:
            log.error("Failed to get Google API credentials")
            return
            
        # Initialize Google API clients
        drive_service = google_api.build('drive', 'v3', credentials=creds)
        gspread_client = None
        try:
            gspread_client = gspread.authorize(creds)
            log.info("Google Sheets API client initialized")
        except Exception as e:
            log.warning(f"Failed to initialize Google Sheets API client: {e}")
            
        # Create base directories
        config.BASE_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        config.STATE_DIR.mkdir(parents=True, exist_ok=True)
        if not args.no_archive:
            config.ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
            
        # Process shared drives first
        shared_processed, shared_downloaded, shared_deleted, shared_failed, processed_drive_ids = process_shared_drives(
            drive_service=drive_service,
            gspread_client=gspread_client,
            s3_client=s3_client,
            s3_bucket=args.s3_bucket,
            s3_prefix=args.s3_prefix,
            incremental_flag=args.incremental,
            dry_run=args.dry_run
        )
        
        # Process My Drive
        my_drive_processed, my_drive_downloaded, my_drive_deleted, my_drive_failed = sync.process_drive(
            drive_service=drive_service,
            gspread_client=gspread_client,
            drive_id=None,  # None for My Drive
            drive_name="My Drive",
            drive_backup_dir=config.BASE_DOWNLOAD_DIR / "My Drive",
            drive_state_dir=config.STATE_DIR / "My Drive",
            processed_shared_drive_ids=processed_drive_ids,
            incremental_flag=args.incremental,
            dry_run=args.dry_run,
            s3_client=s3_client,
            s3_bucket=args.s3_bucket,
            s3_base_prefix=f"{args.s3_prefix.rstrip('/')}/My Drive" if args.s3_prefix else "My Drive"
        )
        
        # Calculate totals
        total_processed = shared_processed + my_drive_processed
        total_downloaded = shared_downloaded + my_drive_downloaded
        total_deleted = shared_deleted + my_drive_deleted
        total_failed = shared_failed + my_drive_failed
        
        # Create archive if requested and there were changes
        if not args.no_archive and (total_downloaded > 0 or total_deleted > 0):
            archive_success, archive_path = archive.create_backup_archive(
                backup_dir=config.BASE_DOWNLOAD_DIR,
                state_dir=config.STATE_DIR,
                s3_enabled=s3_enabled,
                s3_client=s3_client,
                s3_bucket=args.s3_bucket,
                s3_prefix=args.s3_prefix,
                dry_run=args.dry_run
            )
            
            if archive_success and archive_path:
                log.info(f"Backup archived to: {archive_path}")
                
                # Clean up old archives
                if not args.dry_run:
                    archive.cleanup_old_archives(max_age_days=30)
                    
        # Print summary
        log.info("Backup completed:")
        log.info(f"  Total files processed: {total_processed}")
        log.info(f"  Total files downloaded: {total_downloaded}")
        log.info(f"  Total files deleted: {total_deleted}")
        log.info(f"  Total files failed: {total_failed}")
        
    except Exception as e:
        log.error(f"Backup failed: {e}", exc_info=True)
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main()) 