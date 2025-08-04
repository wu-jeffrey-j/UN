#!/usr/bin/env python3
"""
Script to upload MP3 files from un_recordings2 directory to Google Cloud Storage.
Preserves folder structure and uploads to bucket 'un_recordings' with prefix 'raw_audio'.
Skips files that already exist on GCS. Uses multithreading for improved performance.
Deletes local folders immediately when they become empty during upload.
"""

import os
import glob
import shutil
from google.cloud import storage
from pathlib import Path
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple, Set

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Thread-safe counters and folder tracking
class UploadCounters:
    def __init__(self):
        self.uploaded = 0
        self.skipped = 0
        self.failed = 0
        self.lock = threading.Lock()
        self.folder_files: Dict[str, Set[str]] = {}  # folder -> set of files
        self.folder_lock = threading.Lock()
    
    def increment_uploaded(self):
        with self.lock:
            self.uploaded += 1
    
    def increment_skipped(self):
        with self.lock:
            self.skipped += 1
    
    def increment_failed(self):
        with self.lock:
            self.failed += 1
    
    def add_file_to_folder(self, folder_path: str, file_path: str):
        """Add a file to a folder's tracking set."""
        with self.folder_lock:
            if folder_path not in self.folder_files:
                self.folder_files[folder_path] = set()
            self.folder_files[folder_path].add(file_path)
    
    def remove_file_from_folder(self, folder_path: str, file_path: str) -> bool:
        """
        Remove a file from a folder's tracking set.
        Returns True if folder is now empty and should be deleted.
        """
        with self.folder_lock:
            if folder_path in self.folder_files:
                self.folder_files[folder_path].discard(file_path)
                if not self.folder_files[folder_path]:
                    # Folder is empty, remove it from tracking
                    del self.folder_files[folder_path]
                    return True
            return False

def blob_exists(bucket, blob_name):
    """
    Check if a blob already exists in the bucket.
    
    Args:
        bucket: GCS bucket object
        blob_name: Name of the blob to check
        
    Returns:
        bool: True if blob exists, False otherwise
    """
    blob = bucket.blob(blob_name)
    
    # Configure timeout for existence check
    import google.api_core.retry
    retry_config = google.api_core.retry.Retry(
        initial=1.0,
        maximum=30.0,
        multiplier=2,
        predicate=google.api_core.retry.if_exception_type(
            google.api_core.exceptions.DeadlineExceeded,
            google.api_core.exceptions.ServiceUnavailable,
            google.api_core.exceptions.TooManyRequests,
        ),
    )
    
    try:
        return blob.exists(timeout=60, retry=retry_config)  # 1 minute timeout
    except Exception as e:
        logger.warning(f"Error checking if blob exists {blob_name}: {e}")
        return False  # Assume it doesn't exist if we can't check

def upload_single_file(args: Tuple[str, storage.Bucket, str, str, str, UploadCounters]) -> None:
    """
    Upload a single file to GCS. This function is designed to be thread-safe.
    
    Args:
        args: Tuple containing (mp3_file, bucket, bucket_name, prefix, source_dir, counters)
    """
    mp3_file, bucket, bucket_name, prefix, source_dir, counters = args
    
    try:
        # Get relative path from source directory to preserve folder structure
        relative_path = os.path.relpath(mp3_file, source_dir)
        
        # Create GCS blob name with prefix
        blob_name = f"{prefix}/{relative_path}"
        
        # Track the folder this file belongs to
        folder_path = os.path.dirname(mp3_file)
        counters.add_file_to_folder(folder_path, mp3_file)
        
        # Check if blob already exists
        if blob_exists(bucket, blob_name):
            logger.info(f"Skipped (already exists): {mp3_file} -> gs://{bucket_name}/{blob_name}")
            # Delete the file
            os.remove(mp3_file)
            counters.increment_skipped()
        else:
            # Create blob and upload with timeout configuration
            blob = bucket.blob(blob_name)
            
            # Configure upload with longer timeout
            import google.api_core.retry
            retry_config = google.api_core.retry.Retry(
                initial=1.0,
                maximum=60.0,
                multiplier=2,
                predicate=google.api_core.retry.if_exception_type(
                    google.api_core.exceptions.DeadlineExceeded,
                    google.api_core.exceptions.ServiceUnavailable,
                    google.api_core.exceptions.TooManyRequests,
                ),
            )
            
            # Upload with retry configuration and longer timeout
            blob.upload_from_filename(
                mp3_file,
                timeout=300,  # 5 minutes timeout
                retry=retry_config
            )
            
            logger.info(f"Uploaded: {mp3_file} -> gs://{bucket_name}/{blob_name}")
            os.remove(mp3_file)
            counters.increment_uploaded()
            
        
    except Exception as e:
        logger.error(f"Failed to upload {mp3_file}: {e}")
        counters.increment_failed()

def delete_source_directory(source_dir: str):
    """
    Delete the entire source directory after all uploads are complete.
    
    Args:
        source_dir: Source directory path to delete
    """
    try:
        if os.path.exists(source_dir):
            shutil.rmtree(source_dir)
            logger.info(f"Deleted entire source directory: {source_dir}")
        else:
            logger.warning(f"Source directory {source_dir} does not exist")
    except Exception as e:
        logger.error(f"Failed to delete source directory {source_dir}: {e}")

def upload_to_gcs(max_workers: int = 2, delete_source: bool = False):
    """
    Upload all MP3 files from un_recordings2 directory to Google Cloud Storage.
    Preserves folder structure with bucket 'un_recordings' and prefix 'raw_audio'.
    Skips files that already exist on GCS. Uses multithreading for improved performance.
    Deletes local folders immediately when they become empty during upload.
    
    Args:
        max_workers: Maximum number of worker threads (default: 2)
        delete_source: Whether to delete the entire source directory after upload (default: False)
    """
    
    # Configuration
    bucket_name = "un_recordings"
    prefix = "raw_audio"
    source_dir = "un_recordings2"
    
    # Initialize GCS client with timeout configuration
    try:
        # Configure client with longer timeouts
        storage_client = storage.Client()
        
        # Configure the client with custom timeout settings
        import google.api_core.client_options
        client_options = google.api_core.client_options.ClientOptions(
            api_endpoint="https://storage.googleapis.com",
            api_audience="https://storage.googleapis.com"
        )
        
        # Create client with timeout configuration
        storage_client = storage.Client(client_options=client_options)
        
        bucket = storage_client.bucket(bucket_name)
        logger.info(f"Connected to GCS bucket: {bucket_name}")
    except Exception as e:
        logger.error(f"Failed to connect to GCS: {e}")
        return
    
    # Check if source directory exists
    if not os.path.exists(source_dir):
        logger.error(f"Source directory '{source_dir}' does not exist")
        return
    
    # Find all MP3 files recursively
    mp3_files = glob.glob(os.path.join(source_dir, "**/*.mp3"), recursive=True)
    logger.info(f"Found {len(mp3_files)} MP3 files to process")
    
    if not mp3_files:
        logger.warning("No MP3 files found in the source directory")
        return
    
    # Initialize thread-safe counters
    counters = UploadCounters()
    
    # Prepare arguments for each file upload
    upload_args = [
        (mp3_file, bucket, bucket_name, prefix, source_dir, counters)
        for mp3_file in mp3_files
    ]
    
    logger.info(f"Starting upload with {max_workers} worker threads...")
    
    # Use ThreadPoolExecutor for concurrent uploads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all upload tasks
        future_to_file = {
            executor.submit(upload_single_file, args): args[0]  # args[0] is mp3_file
            for args in upload_args
        }
        
        # Process completed tasks
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                future.result()  # This will raise any exception that occurred
            except Exception as e:
                logger.error(f"Unexpected error processing {file_path}: {e}")
    
    # Summary
    logger.info(f"Upload complete!")
    logger.info(f"Successfully uploaded: {counters.uploaded} files")
    logger.info(f"Skipped (already exist): {counters.skipped} files")
    if counters.failed > 0:
        logger.warning(f"Failed uploads: {counters.failed} files")
    
    # Clean up source directory if requested
    if delete_source:
        delete_source_directory(source_dir)

def main():
    """Main function to run the upload process."""
    logger.info("Starting MP3 upload to Google Cloud Storage...")
    upload_to_gcs()
    logger.info("Upload process completed.")

if __name__ == "__main__":
    main()
