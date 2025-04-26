# -*- coding: utf-8 -*-

import logging
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Any

from . import config

log = logging.getLogger(__name__)

def create_backup_archive(
    backup_dir: Path,
    state_dir: Path,
    s3_enabled: bool,
    s3_client: Optional[Any],
    s3_bucket: Optional[str],
    s3_prefix: Optional[str],
    dry_run: bool
) -> Tuple[bool, Optional[Path]]:
    """
    Creates a ZIP archive of the backup directory and optionally uploads it to S3.
    Returns (success, archive_path).
    """
    if dry_run:
        log.info("Skipping archive creation in dry run mode")
        return True, None
        
    try:
        # Create archive filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"drive_backup_{timestamp}.zip"
        archive_path = config.ARCHIVE_DIR / archive_name
        
        # Ensure archive directory exists
        config.ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        
        # Create ZIP archive
        log.info(f"Creating archive: {archive_path}")
        shutil.make_archive(
            str(archive_path.with_suffix("")),  # Remove .zip as make_archive adds it
            "zip",
            backup_dir
        )
        
        # Upload to S3 if enabled
        if s3_enabled and s3_client and s3_bucket:
            try:
                # Construct S3 key
                s3_key = f"{s3_prefix.rstrip('/')}/{archive_name}" if s3_prefix else archive_name
                
                log.info(f"Uploading archive to s3://{s3_bucket}/{s3_key}")
                s3_client.upload_file(
                    str(archive_path),
                    s3_bucket,
                    s3_key
                )
                log.info("Archive uploaded to S3 successfully")
                
            except Exception as e:
                log.error(f"Failed to upload archive to S3: {e}")
                # Continue even if S3 upload fails
                
        return True, archive_path
        
    except Exception as e:
        log.error(f"Failed to create archive: {e}", exc_info=True)
        return False, None

def cleanup_old_archives(
    max_age_days: int = 30,
    dry_run: bool = False
) -> int:
    """
    Removes archives older than max_age_days.
    Returns number of archives removed.
    """
    if dry_run:
        log.info("Skipping archive cleanup in dry run mode")
        return 0
        
    try:
        if not config.ARCHIVE_DIR.exists():
            return 0
            
        removed_count = 0
        current_time = time.time()
        max_age_seconds = max_age_days * 24 * 60 * 60
        
        for archive_file in config.ARCHIVE_DIR.glob("*.zip"):
            try:
                file_age = current_time - archive_file.stat().st_mtime
                if file_age > max_age_seconds:
                    log.info(f"Removing old archive: {archive_file}")
                    archive_file.unlink()
                    removed_count += 1
            except Exception as e:
                log.error(f"Failed to remove old archive {archive_file}: {e}")
                
        if removed_count > 0:
            log.info(f"Removed {removed_count} old archives")
            
        return removed_count
        
    except Exception as e:
        log.error(f"Error during archive cleanup: {e}", exc_info=True)
        return 0 