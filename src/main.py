from typing import Optional, Any
import boto3
import config
import log
import os
import argparse
import logging.handlers
from pathlib import Path
import sys
import platform
import psutil

def setup_logging():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    log_format = "%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / "backup_job.log",
        maxBytes=10*1024*1024,  
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    sys_info_logger = logging.getLogger('system_info')
    sys_info_logger.info(f"Python version: {sys.version}")
    sys_info_logger.info(f"Platform: {platform.platform()}")
    sys_info_logger.info(f"CPU count: {os.cpu_count()}")
    sys_info_logger.info(f"Memory info: {psutil.virtual_memory()}")

def setup_s3_client(s3_bucket: Optional[str], s3_endpoint_url: Optional[str] = None, s3_region: Optional[str] = None, s3_access_key: Optional[str] = None, s3_secret_key: Optional[str] = None) -> tuple[Optional[Any], bool]:
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
        s3_client_kwargs = {}
        
        if s3_endpoint_url:
            s3_client_kwargs['endpoint_url'] = s3_endpoint_url
            
        if s3_region:
            s3_client_kwargs['region_name'] = s3_region
            
        if s3_access_key and s3_secret_key:
            s3_client_kwargs['aws_access_key_id'] = s3_access_key
            s3_client_kwargs['aws_secret_access_key'] = s3_secret_key
            
        s3_client = boto3.client('s3', **s3_client_kwargs)
        log.info(f"S3 client initialized for bucket: {s3_bucket}")
        return s3_client, True
    except Exception as e:
        log.error(f"Failed to initialize S3 client: {e}")
        return None, False 