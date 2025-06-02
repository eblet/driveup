import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Set, Dict, Any

class FileListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.downloaded_files: Set[str] = set()
        self.skipped_files: Set[str] = set()
        self.failed_files: Dict[str, str] = {}  # file_path: error_message

    def emit(self, record):
        pass

class DriveupLogger:
    def __init__(self):
        self.file_handler = None
        self.file_list_handler = FileListHandler()
        self.log_file_path = None

    def setup(self, log_level: str = "INFO"):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_dir = Path("/app/driveup_logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        self.log_file_path = log_dir / f"driveup_{timestamp}.log"
        
        # Create file handler
        self.file_handler = logging.FileHandler(self.log_file_path)
        self.file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_level.upper()))
        root_logger.addHandler(self.file_handler)
        root_logger.addHandler(self.file_list_handler)

    def log_file_status(self, file_path: str, status: str, error_msg: str = None):
        if status == "downloaded":
            self.file_list_handler.downloaded_files.add(file_path)
        elif status == "skipped":
            self.file_list_handler.skipped_files.add(file_path)
        elif status == "failed":
            self.file_list_handler.failed_files[file_path] = error_msg or "Unknown error"

    def write_summary(self):
        if not self.log_file_path or not self.log_file_path.exists():
            return

        with open(self.log_file_path, "a") as f:
            f.write("\n\n" + "="*80 + "\n")
            f.write("BACKUP SUMMARY\n")
            f.write("="*80 + "\n\n")

            f.write("DOWNLOADED FILES:\n")
            f.write("-"*80 + "\n")
            for file in sorted(self.file_list_handler.downloaded_files):
                f.write(f"✓ {file}\n")

            if self.file_list_handler.skipped_files:
                f.write("\nSKIPPED FILES:\n")
                f.write("-"*80 + "\n")
                for file in sorted(self.file_list_handler.skipped_files):
                    f.write(f"⚠ {file}\n")

            if self.file_list_handler.failed_files:
                f.write("\nFAILED FILES:\n")
                f.write("-"*80 + "\n")
                for file_path, error in sorted(self.file_list_handler.failed_files.items()):
                    f.write(f"✗ {file_path}\n")
                    f.write(f"  Error: {error}\n")

            f.write("\n" + "="*80 + "\n")

driveup_logger = DriveupLogger() 