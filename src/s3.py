# -*- coding: utf-8 -*-

import logging
from typing import Optional, Any

from . import config

# Try importing Boto3 for S3
if config.BOTO3_AVAILABLE:
    import boto3
    from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
else:
    # Define dummy exceptions if Boto3 not available, for cleaner except blocks later
    NoCredentialsError = type('NoCredentialsError', (Exception,), {})
    PartialCredentialsError = type('PartialCredentialsError', (Exception,), {})
    ClientError = type('ClientError', (Exception,), {})

log = logging.getLogger(__name__)

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
        
        s3_client_kwargs['config'] = boto3.session.Config(
            signature_version='s3v4',
            s3={
                'addressing_style': 'auto',
                'payload_signing_enabled': False
            }
        )
            
        s3_client = boto3.client('s3', **s3_client_kwargs)
        log.info(f"S3 client initialized for bucket: {s3_bucket}")
        return s3_client, True
    except Exception as e:
        log.error(f"Failed to initialize S3 client: {e}")
        return None, False

def upload_archive_to_s3(archive_path: str, s3_client: Any, s3_bucket: str, s3_prefix: Optional[str], archive_name: str) -> bool:
    """
    Uploads the specified archive file to S3.
    Returns True on success, False on failure.
    """
    try:
        s3_key = f"{s3_prefix.rstrip('/')}/{archive_name}" if s3_prefix else archive_name
        log.info(f"Uploading archive to s3://{s3_bucket}/{s3_key}")
        
        # Read file into memory
        with open(archive_path, 'rb') as f:
            file_data = f.read()
        
        # Upload using put_object
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=file_data
        )
        
        log.info("Archive uploaded to S3 successfully")
        return True
    except (NoCredentialsError, PartialCredentialsError):
        log.error("AWS credentials not found for S3 archive upload. Skipping S3 upload.")
        return False
    except ClientError as e:
        log.error(f"AWS S3 client error uploading archive to s3://{s3_bucket}/{s3_key}: {e}")
        return False
    except Exception as e:
        log.error(f"Unknown error during S3 archive upload to s3://{s3_bucket}/{s3_key}: {e}")
        return False 