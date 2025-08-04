import requests
from bs4 import BeautifulSoup
import os
import time
import re
from urllib.parse import urljoin
import concurrent.futures
import zipfile
from threading import Lock
import csv
import json
import shutil
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import argparse
import logging
from google.cloud import storage
import google.api_core.retry
import google.api_core.client_options

BASE_URL = "https://conf.unog.ch/digitalrecordings/en/clients"

# headers = {"User-Agent": "Mozilla/5.0"}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml",
}

# GCS Configuration
GCS_BUCKET_NAME = "un_recordings"
GCS_PREFIX = "raw_audio"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# CSV tracking file
CSV_FILE = 'scraper_status.csv'
CSV_HEADERS = ['timestamp', 'url', 'filename', 'status', 'duration_seconds', 'error_message']

def write_csv_entry(url, filename, status, duration_seconds, error_message=""):
    """
    Writes a scraping status entry to the CSV file.
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    row = [timestamp, url, filename, status, duration_seconds, error_message]
    
    # Create file with headers if it doesn't exist
    file_exists = os.path.exists(CSV_FILE)
    
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(CSV_HEADERS)
        writer.writerow(row)

def create_resilient_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

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

def extract_and_upload_zip(zip_path, folder_path, bucket):
    """
    Extract a ZIP file and upload all MP3 files to GCS.
    
    Args:
        zip_path: Path to the ZIP file
        folder_path: Path to extract to
        bucket: GCS bucket object
        
    Returns:
        tuple: (success_count, total_count, error_message)
    """
    try:
        logger.info(f"📦 Extracting {zip_path} to {folder_path}")
        
        # Extract ZIP file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(folder_path)
        
        # Find all MP3 files in the extracted directory
        mp3_files = []
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith('.mp3'):
                    mp3_files.append(os.path.join(root, file))
        
        logger.info(f"🎵 Found {len(mp3_files)} MP3 files in {zip_path}")
        
        if not mp3_files:
            return 0, 0, "No MP3 files found in ZIP"
        
        # Upload each MP3 file to GCS
        success_count = 0
        for mp3_file in mp3_files:
            try:
                # Get relative path from the extracted folder
                relative_path = os.path.relpath(mp3_file, folder_path)
                
                if upload_mp3_to_gcs(bucket, mp3_file, relative_path):
                    success_count += 1
                    # Delete the MP3 file after successful upload
                    os.remove(mp3_file)
                
            except Exception as e:
                logger.error(f"❌ Failed to process MP3 file {mp3_file}: {e}")
        
        # Clean up extracted directory
        try:
            shutil.rmtree(folder_path)
            logger.info(f"🗑️ Cleaned up extracted directory: {folder_path}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to clean up directory {folder_path}: {e}")
        
        return success_count, len(mp3_files), ""
        
    except Exception as e:
        error_msg = f"Failed to extract/upload ZIP {zip_path}: {e}"
        logger.error(f"❌ {error_msg}")
        return 0, 0, error_msg


def make_request_with_retries(url, headers, retries=3, delay=5, timeout=120):
    """
    Makes an HTTP GET request with retries for connection and timeout errors.
    """
    start_time = datetime.now()
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
            duration = datetime.now() - start_time
            logger.info(f"✅ Request successful: {url} (took {duration.total_seconds():.2f}s)")
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code // 100 == 5:
                # Server error
                logger.warning(f"⚠️  Attempt {attempt + 1} for {url} failed with {e.response.status_code} Server Error. Retrying in {delay}s...")
                time.sleep(delay)
                continue # Go to the next attempt
            elif e.response.status_code // 100 == 4:
                # Client error
                logger.error(f"❌ 404 Not Found for {url}. Aborting.")
                duration = datetime.now() - start_time
                write_csv_entry(url, "", "REQUEST_404", duration.total_seconds(), f"HTTP {e.response.status_code}")
                return None # Stop and return None
            else:
                # For any other HTTP error, print it and stop.
                logger.error(f"❌ HTTP error for {url}: {e}")
                duration = datetime.now() - start_time
                write_csv_entry(url, "", "REQUEST_HTTP_ERROR", duration.total_seconds(), f"HTTP {e.response.status_code}")
                break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            logger.warning(f"⚠️  Attempt {attempt + 1} for {url} failed with error: {e}. Retrying in {delay}s...")
            time.sleep(delay)
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ An unexpected error occurred for {url}: {e}")
            duration = datetime.now() - start_time
            write_csv_entry(url, "", "REQUEST_EXCEPTION", duration.total_seconds(), str(e))
            break # For other errors, we might not want to retry.
    
    duration = datetime.now() - start_time
    write_csv_entry(url, "", "REQUEST_FAILED", duration.total_seconds(), "Max retries exceeded")
    return None

def request_download(file_path, filename, full_download_url, retries=5):
    start_time = datetime.now()
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"📥 Downloading {filename}... (Attempt {attempt})")
            with requests.get(full_download_url, headers=headers, stream=True, timeout=60) as dl:
                dl.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in dl.iter_content(chunk_size=8192):
                        f.write(chunk)
            duration = datetime.now() - start_time
            logger.info(f"✅ Saved to {file_path} (took {duration.total_seconds():.2f}s)")
            write_csv_entry(full_download_url, filename, "DOWNLOAD_SUCCESS", duration.total_seconds())
            return True  # Success: exit loop
        except (requests.exceptions.RequestException, ConnectionResetError) as e:
            logger.warning(f"⚠️  Attempt {attempt} failed: {e}")
            if attempt == retries:
                logger.error("❌ Max retries reached. Skipping this file.")
                duration = datetime.now() - start_time
                write_csv_entry(full_download_url, filename, "DOWNLOAD_FAILED", duration.total_seconds(), str(e))
                return False # Indicate failure
            else:
                wait_time = 2 ** attempt
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

def get_total_pages(url):
    logger.info(f"🔍 Getting total pages for: {url}")
    start_time = datetime.now()
    
    r = make_request_with_retries(url, headers=headers)
    if not r:
        duration = datetime.now() - start_time
        write_csv_entry(url, "", "PAGE_COUNT_FAILED", duration.total_seconds(), "Request failed")
        return 1

    soup = BeautifulSoup(r.text, 'html.parser')
    page_links = soup.select("ul.pager__items li a.pager__link")
    if not page_links:
        duration = datetime.now() - start_time
        logger.info(f"📄 Single page detected for {url}")
        return 1
    
    max_page = 0
    for link in page_links:
        href = link.get("href", "")
        match = re.search(r"page=(\d+)", href)
        if match:
            page_num = int(match.group(1))
            max_page = max(max_page, page_num)
    
    total_pages = max_page + 1
    duration = datetime.now() - start_time
    logger.info(f"📄 Found {total_pages} pages for {url} (took {duration.total_seconds():.2f}s)")
    return total_pages

def parse_session_links(page_num):
    url = f"{BASE_URL}?page={page_num}"
    logger.info(f"🔗 Parsing session links from page {page_num}")
    start_time = datetime.now()
    
    r = make_request_with_retries(url, headers=headers)
    if not r:
        duration = datetime.now() - start_time
        write_csv_entry(url, "", "SESSION_LINKS_FAILED", duration.total_seconds(), "Request failed")
        return []

    soup = BeautifulSoup(r.text, 'html.parser')

    session_links = []
    rows = soup.select("div.views-row")
    for row in rows:
        a_tag = row.select_one("div.un-box a")
        if not a_tag:
            continue

        href = a_tag.get("href")
        if href and "/digitalrecordings/en/clients/" in href:
            session_id = os.path.split(href)[-1]
            full_url = f"{BASE_URL}/{session_id}/meetings"
            session_links.append(full_url)

    duration = datetime.now() - start_time
    logger.info(f"🔗 Found {len(session_links)} session links on page {page_num} (took {duration.total_seconds():.2f}s)")
    return session_links

def parse_audio_links(session_url, subpage):
    session_subpage_url = f"{session_url}?page={subpage}"
    logger.info(f"🎵 Parsing audio links from {session_url} subpage {subpage}")
    start_time = datetime.now()
    
    r = make_request_with_retries(session_subpage_url, headers=headers)
    if not r:
        duration = datetime.now() - start_time
        write_csv_entry(session_subpage_url, "", "AUDIO_LINKS_FAILED", duration.total_seconds(), "Request failed")
        return [], 0, 0, 0

    soup = BeautifulSoup(r.text, "html.parser")

    audio_links = []

    meetings = soup.select("div.meeting-list-item")
    private_meetings = 0
    unavailable_meetings = 0
    total_meetings = 0
    for meeting in meetings:
        total_meetings += 1
        # Skip private meetings
        is_private = meeting.select_one("span.meeting-list-item--visibility[title='Private meeting']")
        if is_private:
            private_meetings += 1
            continue

        # Extract audio URL from "Listen" button
        listen_link = meeting.select_one("a.button--alt")
        if listen_link and listen_link.get("href"):
            relative_href = listen_link["href"]
            full_url = "https://conf.unog.ch" + relative_href
            audio_links.append(full_url)
        else:
            unavailable_meetings += 1

    duration = datetime.now() - start_time
    logger.info(f"🎵 Found {len(audio_links)} audio links, {private_meetings} private, {unavailable_meetings} unavailable out of {total_meetings} total meetings (took {duration.total_seconds():.2f}s)")
    return audio_links, private_meetings, unavailable_meetings, total_meetings

def download_zip(url, save_folder="./un_recordings", bucket=None):
    logger.info(f"📦 Processing ZIP download for: {url}")
    start_time = datetime.now()
    
    os.makedirs(save_folder, exist_ok=True)

    r = make_request_with_retries(url, headers=headers)
    if not r:
        duration = datetime.now() - start_time
        write_csv_entry(url, "", "ZIP_PROCESSING_FAILED", duration.total_seconds(), "Request failed")
        logger.error(f"❌ Failed to process URL: {url}")
        return

    soup = BeautifulSoup(r.text, "html.parser")
    download_link = soup.select_one("a#download-all")

    if not download_link:
        logger.warning(f"⚠️  No ZIP download found on: {url}")
        duration = datetime.now() - start_time
        write_csv_entry(url, "", "ZIP_NOT_FOUND", duration.total_seconds(), "No download link found")
        return

    href = download_link.get("href")
    filename = download_link.get("download", "session.zip")
    date_text = soup.select_one("span.meeting-details--date").get_text(strip=True)
    time_text = soup.select_one("span.meeting-details--time").get_text(strip=True)  
    full_download_url = urljoin(BASE_URL, href)

    # Add date and time to folder name
    folder_path = os.path.join(save_folder, filename.split(".")[0])
    folder_path = f"{folder_path}_{date_text}_{time_text}"
    
    # Check if this session already exists on GCS before downloading
    if bucket:
        try:
            # Create a sample blob name to check if this session exists
            sample_blob_name = f"{GCS_PREFIX}/{os.path.basename(folder_path)}/ORIGINAL.mp3"
            if blob_exists(bucket, sample_blob_name):
                logger.info(f"⏭️ Session already exists on GCS: {folder_path}")
                duration = datetime.now() - start_time
                write_csv_entry(url, filename, "ALREADY_ON_GCS", duration.total_seconds(), "Session already uploaded")
                return
        except Exception as e:
            logger.warning(f"⚠️ Error checking GCS for existing session: {e}")

    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, filename)

    # Stream download to file
    download_success = request_download(file_path, filename, full_download_url)
    
    if not download_success:
        duration = datetime.now() - start_time
        write_csv_entry(url, filename, "DOWNLOAD_FAILED", duration.total_seconds(), "Download failed")
        logger.error(f"❌ Failed to download ZIP: {url}")
        return

    # Extract and upload to GCS
    if bucket:
        try:
            success_count, total_count, error_msg = extract_and_upload_zip(file_path, folder_path, bucket)
            
            if success_count > 0:
                logger.info(f"✅ Successfully uploaded {success_count}/{total_count} MP3 files to GCS")
                # Delete the folder the ZIP file is in
                try:
                    shutil.rmtree(folder_path)
                    logger.info(f"🗑️ Deleted ZIP file: {file_path}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to delete ZIP file {file_path}: {e}")
            else:
                logger.error(f"❌ Failed to upload any MP3 files: {error_msg}")
                
        except Exception as e:
            logger.error(f"❌ Failed to extract/upload ZIP {file_path}: {e}")
            shutil.rmtree(folder_path)
            error_msg = str(e)
    else:
        logger.warning(f"⚠️ No GCS bucket available, skipping upload")
        error_msg = "No GCS bucket available"
    
    duration = datetime.now() - start_time
    logger.info(f"📦 ZIP processing completed for {url} (total time: {duration.total_seconds():.2f}s)")
    
    # Log final status
    if 'error_msg' in locals() and error_msg:
        write_csv_entry(url, filename, "UPLOAD_FAILED", duration.total_seconds(), error_msg)
    else:
        write_csv_entry(url, filename, "SUCCESS", duration.total_seconds(), "")

def process_session(session_url, bucket=None):
    """
    Processes a single session URL, finds audio links across its subpages,
    and downloads the corresponding zip files.
    """
    logger.info(f"🚀 Starting session processing: {session_url}")
    session_start_time = datetime.now()
    
    total_subpages = get_total_pages(session_url)
    successful_downloads = 0
    failed_downloads = 0
    
    for subpage in range(0, total_subpages):
        logger.info(f"📄 Processing subpage {subpage+1}/{total_subpages} of {session_url}")
        try:
            audio_links, private, unavailable, total = parse_audio_links(session_url, subpage)
            for audio_url in audio_links:
                try:
                    download_zip(audio_url, bucket=bucket)
                    successful_downloads += 1
                except Exception as e:
                    logger.error(f"❌ Failed to download ZIP from {audio_url}: {e}")
                    failed_downloads += 1
                    # Log the failed URL
                    duration = datetime.now() - session_start_time
                    write_csv_entry(audio_url, "", "SESSION_PROCESSING_FAILED", duration.total_seconds(), str(e))
                time.sleep(1)
        except Exception as e:
            logger.error(f"❌ Error processing subpage {subpage} for {session_url}: {e}")
            failed_downloads += 1
            # Log the failed URL
            duration = datetime.now() - session_start_time
            write_csv_entry(session_url, "", "SUBPAGE_PROCESSING_FAILED", duration.total_seconds(), str(e))
        time.sleep(1)  # Be polite to the server
    
    session_duration = datetime.now() - session_start_time
    logger.info(f"🏁 Session completed: {session_url}")
    logger.info(f"📊 Summary: {successful_downloads} successful downloads, {failed_downloads} failed")
    logger.info(f"⏱️  Total session time: {session_duration.total_seconds():.2f}s")

# Main Scraper Loop
if __name__ == "__main__":
    logger.info("🎬 Starting UN recordings scraper")
    total_start_time = datetime.now()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--max_workers", type=int, default=7)
    args = parser.parse_args()
    max_workers = args.max_workers

    # Initialize GCS client
    storage_client, bucket = initialize_gcs_client()
    if not bucket:
        logger.warning("⚠️ GCS not available, will only download files locally")

    VM_ID = os.getenv("VM_ID")
    if VM_ID == "1":
        pages = [0,2,4,6]
    elif VM_ID == "2":
        pages = [1,3,5,7,8]
    # pages = [8]

    all_session_links = []
    for page in pages:
        session_links = parse_session_links(page)
        if session_links:
            all_session_links.extend(session_links)
        else:
            logger.error(f"❌ Failed to parse session links for page {page}")

    logger.info(f"📊 Found {len(all_session_links)} sessions to process.")

    successful_sessions = 0
    failed_sessions = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Pass bucket to each session processing task
        future_to_url = {executor.submit(process_session, url, bucket): url for url in all_session_links}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()
                successful_sessions += 1
            except Exception as exc:
                logger.error(f"❌ {url} generated an exception: {exc}")
                failed_sessions += 1
                # Log the failed URL
                duration = datetime.now() - total_start_time
                write_csv_entry(url, "", "SESSION_EXCEPTION", duration.total_seconds(), str(exc))
    
    total_duration = datetime.now() - total_start_time
    logger.info(f"🎉 Scraping complete!")
    logger.info(f"📈 Final Summary: {successful_sessions} successful sessions, {failed_sessions} failed sessions")
    logger.info(f"⏱️  Total execution time: {total_duration.total_seconds():.2f}s")

