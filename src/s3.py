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
    Uploads the specified archive file to S3 using multipart upload for large files.
    Returns True on success, False on failure.
    """
    import os
    from pathlib import Path
    
    try:
        s3_key = f"{s3_prefix.rstrip('/')}/{archive_name}" if s3_prefix else archive_name
        file_size = Path(archive_path).stat().st_size
        log.info(f"Uploading archive to s3://{s3_bucket}/{s3_key} (size: {file_size / (1024*1024*1024):.2f} GB)")
        
        # Use multipart upload for files larger than 100MB
        if file_size > 100 * 1024 * 1024:  # 100MB threshold
            log.info("Using multipart upload for large file")
            return _multipart_upload(s3_client, archive_path, s3_bucket, s3_key, file_size)
        else:
            log.info("Using simple upload for small file")
            return _simple_upload(s3_client, archive_path, s3_bucket, s3_key)
            
    except (NoCredentialsError, PartialCredentialsError) as e:
        log.error(f"AWS credentials not found for S3 archive upload: {e}")
        return False
    except ClientError as e:
        log.error(f"AWS S3 client error uploading archive to s3://{s3_bucket}/{s3_key}: {e}")
        return False
    except Exception as e:
        log.error(f"Unknown error during S3 archive upload to s3://{s3_bucket}/{s3_key}: {e}", exc_info=True)
        return False

def _simple_upload(s3_client: Any, archive_path: str, s3_bucket: str, s3_key: str) -> bool:
    """Simple upload for small files"""
    with open(archive_path, 'rb') as f:
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=f.read()
        )
    log.info("Archive uploaded to S3 successfully (simple upload)")
    return True

def _multipart_upload(s3_client: Any, archive_path: str, s3_bucket: str, s3_key: str, file_size: int) -> bool:
    """Multipart upload for large files"""
    chunk_size = 100 * 1024 * 1024  # 100MB chunks
    
    # Start multipart upload
    response = s3_client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key)
    upload_id = response['UploadId']
    log.info(f"Started multipart upload with ID: {upload_id}")
    
    parts = []
    part_number = 1
    
    try:
        with open(archive_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                
                # Upload part
                log.info(f"Uploading part {part_number} ({len(chunk) / (1024*1024):.1f} MB)")
                response = s3_client.upload_part(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )
                
                parts.append({
                    'ETag': response['ETag'],
                    'PartNumber': part_number
                })
                part_number += 1
        
        # Complete multipart upload
        s3_client.complete_multipart_upload(
            Bucket=s3_bucket,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        
        log.info(f"Archive uploaded to S3 successfully (multipart upload, {len(parts)} parts)")
        return True
        
    except Exception as e:
        # Abort multipart upload on error
        log.error(f"Multipart upload failed, aborting: {e}")
        try:
            s3_client.abort_multipart_upload(
                Bucket=s3_bucket,
                Key=s3_key,
                UploadId=upload_id
            )
            log.info("Multipart upload aborted")
        except Exception as abort_e:
            log.error(f"Failed to abort multipart upload: {abort_e}")
        raise 