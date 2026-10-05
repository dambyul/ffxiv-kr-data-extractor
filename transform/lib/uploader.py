import os
import boto3
from .logging_setup import get_logger

logger = get_logger()

class S3Uploader:
    def __init__(self, bucket_name=None):
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME", "ff14-kr-csv")
        self.s3 = None
        try:
            self.s3 = boto3.client('s3')
        except Exception as e:
            logger.warning(f"Failed to initialize Boto3 client: {e}")

    def upload_files(self, file_paths, key_prefix=""):
        if not self.s3:
            logger.warning("S3 Client not available. Skipping upload.")
            return False
            
        logger.info("S3 Uploading...")
        success = True
        for f in file_paths:
            if os.path.exists(f):
                try:
                    logger.info(f"  Uploading {os.path.basename(f)}...")
                    key = '/'.join(part for part in (key_prefix.strip('/'), os.path.basename(f)) if part)
                    # Mutable latest-release objects must be revalidated by CloudFront/browsers.
                    if os.path.basename(f) in ('resources.zip', 'data.json', 'version.txt', 'dev-version.txt'):
                        self.s3.upload_file(f, self.bucket_name, key,
                                            ExtraArgs={'CacheControl': 'no-cache, max-age=0, must-revalidate'})
                    else:
                        self.s3.upload_file(f, self.bucket_name, key)
                except Exception as e:
                    logger.error(f"  Failed to upload {os.path.basename(f)}: {e}")
                    success = False
            else:
                logger.error(f"  Upload source missing: {os.path.basename(f)}")
                success = False
        return success

    def cleanup_local(self, file_paths):
        logger.info("Cleaning up temporary local files...")
        for f in file_paths:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except Exception as e:
                    logger.warning(f"  Failed to delete {f}: {e}")
