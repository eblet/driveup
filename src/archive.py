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
    # state_dir: Path, # No longer needed as state is part of backup_dir for simplicity in archiving
    # s3_enabled: bool, # S3 logic is now handled externally
    # s3_client: Optional[Any], # S3 logic is now handled externally
    # s3_bucket: Optional[str], # S3 logic is now handled externally
    # s3_prefix: Optional[str], # S3 logic is now handled externally
    dry_run: bool,
    mode: str = "full"
) -> Tuple[bool, Optional[Path], Optional[str]]: # Returns success, archive_path, archive_name
    """
    Creates a ZIP archive of the backup directory.
    Returns (success, archive_path, archive_name).
    """
    if dry_run:
        log.info("Skipping archive creation in dry run mode")
        return True, None, None
        
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"drive_backup_{timestamp}_{mode}.zip"
        archive_path = config.ARCHIVE_DIR / archive_name
        
        config.ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        
        log.info(f"Creating archive: {archive_path} from directory {backup_dir}")
        shutil.make_archive(
            str(archive_path.with_suffix("")),  # Remove .zip as make_archive adds it
            "zip",
            root_dir=backup_dir, # Archive contents of backup_dir
            base_dir="." # Archive all files/folders within backup_dir itself
        )
        
        # S3 upload logic is removed from here
                
        return True, archive_path, archive_name
        
    except Exception as e:
        log.error(f"Failed to create archive: {e}", exc_info=True)
        return False, None, None

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