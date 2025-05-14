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
from src import s3

# Initialize logger using the config setup
log = logging.getLogger(__name__)

# --- Try importing Boto3 for S3 (this is now handled in s3.py, but keep for dummy exceptions) ---
if config.BOTO3_AVAILABLE:
    import boto3 # boto3 import itself is fine here
    from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
else:
    NoCredentialsError = type('NoCredentialsError', (Exception,), {})
    PartialCredentialsError = type('PartialCredentialsError', (Exception,), {})
    ClientError = type('ClientError', (Exception,), {})

def process_shared_drives(
    drive_service: Resource,
    gspread_client: Optional[gspread.Client],
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
                dry_run=dry_run
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
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
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
        shared_processed, shared_downloaded, shared_deleted, shared_failed, processed_drive_ids, shared_modes = 0, 0, 0, 0, set(), []
        try:
            # List all shared drives
            drives_result = drive_service.drives().list(
                pageSize=100,
                fields="drives(id, name)"
            ).execute()
            drives = drives_result.get('drives', [])
            log.info(f"Found {len(drives)} shared drives")
            for drive in drives:
                drive_id = drive['id']
                drive_name = drive['name']
                safe_drive_name = utils.sanitize_filename(drive_name)
                drive_backup_dir = config.BASE_DOWNLOAD_DIR / safe_drive_name
                drive_state_dir = config.STATE_DIR / safe_drive_name
                drive_backup_dir.mkdir(parents=True, exist_ok=True)
                drive_state_dir.mkdir(parents=True, exist_ok=True)
                processed, downloaded, deleted, failed, mode = sync.process_drive(
                    drive_service=drive_service,
                    gspread_client=gspread_client,
                    drive_id=drive_id,
                    drive_name=drive_name,
                    drive_backup_dir=drive_backup_dir,
                    drive_state_dir=drive_state_dir,
                    processed_shared_drive_ids=processed_drive_ids,
                    incremental_flag=args.incremental,
                    dry_run=args.dry_run
                )
                shared_processed += processed
                shared_downloaded += downloaded
                shared_deleted += deleted
                shared_failed += failed
                processed_drive_ids.add(drive_id)
                shared_modes.append(mode)
        except Exception as e:
            log.error(f"Error processing shared drives: {e}", exc_info=True)
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
        # Определяем итоговый режим для архива
        all_modes = shared_modes + [my_drive_mode]
        archive_mode = "full" if "full" in all_modes else "incremental"
        # Create archive if requested and there were changes
        if not args.no_archive and (total_downloaded > 0 or total_deleted > 0):
            archive_created, archive_path, archive_name = archive.create_backup_archive(
                backup_dir=config.BASE_DOWNLOAD_DIR,
                dry_run=args.dry_run,
                mode=archive_mode
            )
            if archive_created and archive_path and archive_name:
                log.info(f"Backup archived locally to: {archive_path}")
                if s3_enabled and s3_client:
                    s3_upload_success = s3.upload_archive_to_s3(
                        archive_path=str(archive_path),
                        s3_client=s3_client,
                        s3_bucket=args.s3_bucket,
                        s3_prefix=args.s3_prefix,
                        archive_name=archive_name
                    )
                    if not s3_upload_success:
                        log.error("Failed to upload archive to S3. The archive remains available locally.")
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