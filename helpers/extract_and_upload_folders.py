#!/usr/bin/env python3
"""
Script to extract folders, upload MP3 files to GCS, and delete local folders after upload.
Takes a local directory full of subdirectories, expands them, uploads MP3s to GCS, and cleans up.
"""

import os
import glob
import shutil
import zipfile
from google.cloud import storage
from pathlib import Path
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple, Set
import google.api_core.retry
import google.api_core.client_options

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# GCS Configuration
GCS_BUCKET_NAME = "un_recordings"
GCS_PREFIX = "raw_audio"

# Thread-safe counters and tracking
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

def initialize_gcs_client():
    """
    Initialize Google Cloud Storage client with timeout configuration.
    """
    try:
        # Configure the client with custom timeout settings
        client_options = google.api_core.client_options.ClientOptions(
            api_endpoint="https://storage.googleapis.com",
            api_audience="https://storage.googleapis.com"
        )
        
        # Create client with timeout configuration
        storage_client = storage.Client(client_options=client_options)
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        logger.info(f"✅ Connected to GCS bucket: {GCS_BUCKET_NAME}")
        return storage_client, bucket
    except Exception as e:
        logger.error(f"❌ Failed to connect to GCS: {e}")
        return None, None

def blob_exists(bucket, blob_name):
    """
    Check if a blob already exists in the bucket with timeout configuration.
    
    Args:
        bucket: GCS bucket object
        blob_name: Name of the blob to check
        
    Returns:
        bool: True if blob exists, False otherwise
    """
    if not bucket:
        return False
        
    blob = bucket.blob(blob_name)
    
    # Configure timeout for existence check
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
        logger.warning(f"⚠️ Error checking if blob exists {blob_name}: {e}")
        return False  # Assume it doesn't exist if we can't check

def upload_mp3_to_gcs(bucket, mp3_file, relative_path):
    """
    Upload an MP3 file to GCS with timeout and retry configuration.
    
    Args:
        bucket: GCS bucket object
        mp3_file: Path to the MP3 file
        relative_path: Relative path for the blob name
        
    Returns:
        bool: True if upload successful, False otherwise
    """
    if not bucket:
        return False
        
    try:
        # Create GCS blob name with prefix
        blob_name = f"{GCS_PREFIX}/{relative_path}"
        
        # Check if blob already exists
        if blob_exists(bucket, blob_name):
            logger.info(f"⏭️ Skipped (already exists): {mp3_file} -> gs://{GCS_BUCKET_NAME}/{blob_name}")
            return True
        
        # Create blob and upload with timeout configuration
        blob = bucket.blob(blob_name)
        
        # Configure upload with longer timeout
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
        
        logger.info(f"☁️ Uploaded: {mp3_file} -> gs://{GCS_BUCKET_NAME}/{blob_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to upload {mp3_file}: {e}")
        return False

def extract_zip_file(zip_path, extract_dir):
    """
    Extract a ZIP file to the specified directory.
    
    Args:
        zip_path: Path to the ZIP file
        extract_dir: Directory to extract to
        
    Returns:
        bool: True if extraction successful, False otherwise
    """
    try:
        logger.info(f"📦 Extracting {zip_path} to {extract_dir}")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        logger.info(f"✅ Successfully extracted {zip_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to extract {zip_path}: {e}")
        return False

def delete_folder_if_empty(folder_path: str):
    """
    Delete a folder after all its files have been processed.
    
    Args:
        folder_path: Path to the folder to delete
    """
    try:
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            # Delete the folder regardless of whether it's empty
            shutil.rmtree(folder_path)
            logger.info(f"🗑️ Deleted folder: {folder_path}")
            return True
                
    except Exception as e:
        logger.error(f"❌ Failed to delete folder {folder_path}: {e}")
        return False

def check_folder_exists_on_gcs(bucket, folder_name):
    """
    Check if a folder/subdirectory already exists on GCS by looking for any files with that prefix.
    
    Args:
        bucket: GCS bucket object
        folder_name: Name of the folder to check
        
    Returns:
        bool: True if folder exists on GCS, False otherwise
    """
    if not bucket:
        return False
        
    try:
        # Create a sample blob name to check if this folder exists
        sample_blob_name = f"{GCS_PREFIX}/{folder_name}/sample.mp3"
        
        # List blobs with the folder prefix to see if any exist
        blobs = bucket.list_blobs(prefix=f"{GCS_PREFIX}/{folder_name}/", max_results=1)
        
        # If we get any results, the folder exists
        for blob in blobs:
            return True
            
        return False
        
    except Exception as e:
        logger.warning(f"⚠️ Error checking if folder exists on GCS {folder_name}: {e}")
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
        
        # Upload to GCS
        if upload_mp3_to_gcs(bucket, mp3_file, relative_path):
            counters.increment_uploaded()
        else:
            counters.increment_failed()
        
        # Remove file from folder tracking and check if folder should be deleted
        if counters.remove_file_from_folder(folder_path, mp3_file):
            # Folder is now empty, try to delete it
            delete_folder_if_empty(folder_path)
        
    except Exception as e:
        logger.error(f"❌ Failed to process {mp3_file}: {e}")
        counters.increment_failed()
        
        # Still remove file from folder tracking even if upload failed
        folder_path = os.path.dirname(mp3_file)
        if counters.remove_file_from_folder(folder_path, mp3_file):
            delete_folder_if_empty(folder_path)

def process_folder(folder_path, bucket, max_workers=8):
    """
    Process a single folder: check GCS first, extract if needed, upload MP3s, and clean up.
    
    Args:
        folder_path: Path to the folder to process
        bucket: GCS bucket object
        max_workers: Maximum number of worker threads
        
    Returns:
        tuple: (uploaded_count, failed_count, total_files, skipped_reason)
    """
    folder_name = os.path.basename(folder_path)
    logger.info(f"📁 Processing folder: {folder_path}")
    
    # First check if this folder already exists on GCS
    if check_folder_exists_on_gcs(bucket, folder_name):
        logger.info(f"⏭️ Folder already exists on GCS: {folder_name}")
        shutil.rmtree(folder_path)
        return 0, 0, 0, "already_exists_on_gcs"
    
    # Check if folder contains ZIP files
    zip_files = glob.glob(os.path.join(folder_path, "*.zip"))
    
    if zip_files:
        # Extract ZIP files first
        for zip_file in zip_files:
            extract_dir = os.path.splitext(zip_file)[0]  # Extract to folder with same name
            if extract_zip_file(zip_file, extract_dir):
                # Delete the ZIP file after successful extraction
                try:
                    os.remove(zip_file)
                    logger.info(f"🗑️ Deleted ZIP file: {zip_file}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to delete ZIP file {zip_file}: {e}")
            else:
                shutil.rmtree(folder_path)
    
    # Find all MP3 files in the folder and subfolders
    mp3_files = glob.glob(os.path.join(folder_path, "**/*.mp3"), recursive=True)
    
    if not mp3_files:
        logger.info(f"📁 No MP3 files found in {folder_path}")
        return 0, 0, 0, "no_mp3_files"
    
    logger.info(f"🎵 Found {len(mp3_files)} MP3 files in {folder_path}")
    
    # Initialize counters for this folder
    counters = UploadCounters()
    
    # Prepare arguments for each file upload
    upload_args = [
        (mp3_file, bucket, GCS_BUCKET_NAME, GCS_PREFIX, folder_path, counters)
        for mp3_file in mp3_files
    ]
    
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
                logger.error(f"❌ Unexpected error processing {file_path}: {e}")
    
    return counters.uploaded, counters.failed, len(mp3_files), "completed"

def extract_and_upload_folders(source_dir, max_workers=8, delete_source=False):
    """
    Extract folders, upload MP3 files to GCS, and delete local folders after upload.
    
    Args:
        source_dir: Source directory containing subdirectories to process
        max_workers: Maximum number of worker threads per folder
        delete_source: Whether to delete the entire source directory after processing
    """
    
    # Initialize GCS client
    storage_client, bucket = initialize_gcs_client()
    if not bucket:
        logger.error("❌ GCS not available, cannot proceed")
        return
    
    # Check if source directory exists
    if not os.path.exists(source_dir):
        logger.error(f"❌ Source directory '{source_dir}' does not exist")
        return
    
    # Find all subdirectories
    subdirs = []
    for item in os.listdir(source_dir):
        item_path = os.path.join(source_dir, item)
        if os.path.isdir(item_path):
            subdirs.append(item_path)
    
    if not subdirs:
        logger.warning(f"⚠️ No subdirectories found in {source_dir}")
        return
    
    logger.info(f"📁 Found {len(subdirs)} subdirectories to process")
    
    # Process each subdirectory
    total_uploaded = 0
    total_failed = 0
    total_files = 0
    processed_folders = 0
    skipped_folders = 0
    skipped_reasons = {}
    
    for subdir in subdirs:
        try:
            uploaded, failed, files, reason = process_folder(subdir, bucket, max_workers)
            
            if reason == "completed":
                total_uploaded += uploaded
                total_failed += failed
                total_files += files
                processed_folders += 1
                logger.info(f"✅ Completed folder {processed_folders}/{len(subdirs)}: {os.path.basename(subdir)}")
            else:
                skipped_folders += 1
                if reason not in skipped_reasons:
                    skipped_reasons[reason] = 0
                skipped_reasons[reason] += 1
                logger.info(f"⏭️ Skipped folder {os.path.basename(subdir)}: {reason}")
            
        except Exception as e:
            logger.error(f"❌ Failed to process folder {subdir}: {e}")
    
    # Summary
    logger.info("📊 EXTRACTION AND UPLOAD SUMMARY")
    logger.info("=" * 50)
    logger.info(f"📁 Processed folders: {processed_folders}/{len(subdirs)}")
    logger.info(f"⏭️ Skipped folders: {skipped_folders}")
    
    if skipped_reasons:
        logger.info("📋 Skip reasons:")
        for reason, count in skipped_reasons.items():
            logger.info(f"   {reason}: {count} folders")
    
    logger.info(f"☁️ Successfully uploaded: {total_uploaded} files")
    logger.info(f"❌ Failed uploads: {total_failed} files")
    logger.info(f"📄 Total files processed: {total_files}")
    
    # Clean up source directory if requested
    if delete_source:
        try:
            shutil.rmtree(source_dir)
            logger.info(f"🗑️ Deleted entire source directory: {source_dir}")
        except Exception as e:
            logger.error(f"❌ Failed to delete source directory {source_dir}: {e}")

def main():
    """
    Main function to run the extraction and upload process.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract folders, upload MP3s to GCS, and clean up')
    parser.add_argument('source_dir', help='Source directory containing subdirectories to process')
    parser.add_argument('--max_workers', type=int, default=8, help='Maximum worker threads per folder (default: 8)')
    parser.add_argument('--delete_source', action='store_true', help='Delete entire source directory after processing')
    
    args = parser.parse_args()
    
    logger.info("🎬 Starting folder extraction and upload process")
    extract_and_upload_folders(args.source_dir, args.max_workers, args.delete_source)
    logger.info("✅ Process completed!")

if __name__ == "__main__":
    main() 