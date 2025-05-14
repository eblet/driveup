from typing import Optional, Any
import boto3
import config
import log

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