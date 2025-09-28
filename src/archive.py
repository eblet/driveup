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
    dry_run: bool,
    mode: str = "full"
) -> Tuple[bool, Optional[Path], Optional[str]]: # Returns success, archive_path, archive_name
    """
    Creates a ZIP archive of the backup directory.
    For dry-run mode, creates a small test archive to verify S3 upload functionality.
    Returns (success, archive_path, archive_name).
    """
    if dry_run:
        log.info("Creating small test archive for dry-run S3 upload verification")
        return _create_test_archive(mode)
        
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

def _create_test_archive(mode: str = "full") -> Tuple[bool, Optional[Path], Optional[str]]:
    """
    Creates a small test archive for dry-run S3 upload verification.
    Returns (success, archive_path, archive_name).
    """
    import tempfile
    import json
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"drive_backup_{timestamp}_{mode}_test.zip"
        archive_path = config.ARCHIVE_DIR / archive_name
        
        config.ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        
        # Create temporary directory with test content
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create test files
            test_info = {
                "test_type": "dry_run_s3_upload_verification",
                "timestamp": timestamp,
                "mode": mode,
                "message": "This is a test archive created during dry-run to verify S3 upload functionality",
                "size": "small"
            }
            
            # Write test JSON file
            with open(temp_path / "test_info.json", "w") as f:
                json.dump(test_info, f, indent=2)
            
            # Write test text file
            with open(temp_path / "test_content.txt", "w") as f:
                f.write("This is a test archive for S3 upload verification.\n")
                f.write(f"Created at: {timestamp}\n")
                f.write(f"Mode: {mode}\n")
                f.write("If you see this file in S3, the upload is working correctly!\n")
            
            # Create small test folder structure
            (temp_path / "test_folder").mkdir()
            with open(temp_path / "test_folder" / "nested_file.txt", "w") as f:
                f.write("Test file in subfolder\n")
            
            log.info(f"Creating test archive: {archive_path}")
            shutil.make_archive(
                str(archive_path.with_suffix("")),  # Remove .zip as make_archive adds it
                "zip",
                root_dir=temp_path,
                base_dir="."
            )
        
        # Check archive size
        archive_size = archive_path.stat().st_size
        log.info(f"Test archive created successfully: {archive_name} ({archive_size} bytes)")
        
        return True, archive_path, archive_name
        
    except Exception as e:
        log.error(f"Failed to create test archive: {e}", exc_info=True)
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