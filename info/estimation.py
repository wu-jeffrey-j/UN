import requests
from bs4 import BeautifulSoup
import os
import time
import re
from urllib.parse import urljoin
import concurrent.futures
import zipfile
from selenium import webdriver
from threading import Lock
import csv
from datetime import datetime
from collections import defaultdict

BASE_URL = "https://conf.unog.ch/digitalrecordings/en/clients"

headers = {"User-Agent": "Mozilla/5.0"}

def make_request_with_retries(url, headers, retries=3, delay=5, timeout=120):
    """
    Makes an HTTP GET request with retries for connection and timeout errors.
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code // 100 == 5:
                # Server error
                print(f"Attempt {attempt + 1} for {url} failed with 503 Server Error. Retrying in {delay}s...")
                time.sleep(delay)
                continue # Go to the next attempt
            elif e.response.status_code // 100 == 4:
                # Client error
                print(f"404 Not Found for {url}. Aborting.")
                return None # Stop and return None
            else:
                # For any other HTTP error, print it and stop.
                print(f"HTTP error for {url}: {e}")
                break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"Attempt {attempt + 1} for {url} failed with error: {e}. Retrying in {delay}s...")
            time.sleep(delay)
        except requests.exceptions.RequestException as e:
            print(f"An unexpected error occurred for {url}: {e}")
            break # For other errors, we might not want to retry.
    return None

def get_total_pages(url):
    r = make_request_with_retries(url, headers=headers)
    if not r:
        return 1

    soup = BeautifulSoup(r.text, 'html.parser')
    page_links = soup.select("ul.pager__items li a.pager__link")
    if not page_links:
        return 1
    max_page = 0
    for link in page_links:
        href = link.get("href", "")
        match = re.search(r"page=(\d+)", href)
        if match:
            page_num = int(match.group(1))
            max_page = max(max_page, page_num)
    return max_page+1 # Second to last link = last numbered page

def parse_session_links(page_num):
    url = f"{BASE_URL}?page={page_num}"
    r = make_request_with_retries(url, headers=headers)
    if not r:
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

    return session_links

def parse_audio_links(session_url, subpage):
    session_subpage_url = f"{session_url}?page={subpage}"
    r = make_request_with_retries(session_subpage_url, headers=headers)
    if not r:
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
            # print(f"Skipping private meeting from {session_subpage_url}")
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
            #print(f"Skipping unavailable meeting from {session_subpage_url}")

    return audio_links, private_meetings, unavailable_meetings, total_meetings

def estimate_hours_by_language(url):
    """
    Estimate hours for each language available in the recording.
    
    Returns:
        dict: Dictionary with language codes as keys and hours as values
    """
    r = make_request_with_retries(url, headers=headers)
    if not r:
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    lang_links = soup.select("div.language-selector a")
    
    # Filter out non-downloadable languages (like the 10h00 link)
    downloadable_langs = [
        a for a in lang_links if not a.get("href", "").endswith("/10h00")
    ]
    
    if not downloadable_langs:
        return {}
    
    # Get the duration from the marker list
    rows = soup.select("#marker-list tr")
    if not rows:
        print(f"No marker list found for {url}")
        return {}
    
    last_row = rows[-1]
    marker_time_cell = last_row.select_one("td.col--marker-time")
    if not marker_time_cell:
        print(f"No marker time cell found for {url}")
        return {}
    
    marker_text = marker_time_cell.get_text(strip=True)
    parts = list(map(int, marker_text.split(":")))
    
    # Calculate duration in hours
    if len(parts) == 1:
        hours = parts[0]
    elif len(parts) == 2:
        hours = parts[0] + parts[1]/60
    elif len(parts) == 3:
        hours = parts[0] + parts[1]/60 + parts[2]/3600
    else:
        print(f"Unexpected time format: {marker_text}")
        return {}
    
    # Create dictionary with hours for each language
    language_hours = {}
    for lang_link in downloadable_langs:
        # Extract language code from href
        href = lang_link.get("href", "")
        # Language code is typically the last part of the URL path
        lang_code = href.split("/")[-1] if href else "unknown"
        language_hours[lang_code] = hours
    
    return language_hours

def estimate_hours(url):
    """
    Legacy function for backward compatibility.
    Returns total hours across all languages.
    """
    language_hours = estimate_hours_by_language(url)
    return sum(language_hours.values())

def process_session(session_url):
    """
    Processes a single session URL, finds audio links across its subpages,
    and estimates hours for each language.
    """
    print(f"Parsing session {session_url}")
    total_subpages = get_total_pages(session_url)
    hours_sum = 0.0
    private_meetings = 0
    unavailable_meetings = 0
    total_meetings = 0
    session_language_hours = defaultdict(float)  # Track hours per language for this session
    
    for subpage in range(0, total_subpages):
        print(f"    Parsing subpage {subpage+1}/{total_subpages} of {session_url}")
        try:
            audio_links, private, unavailable, total = parse_audio_links(session_url, subpage)
            for audio_url in audio_links:
                language_hours = estimate_hours_by_language(audio_url)
                hours_sum += sum(language_hours.values())
                
                # Accumulate hours per language
                for lang, hours in language_hours.items():
                    session_language_hours[lang] += hours
                
                time.sleep(1)
            private_meetings += private
            unavailable_meetings += unavailable
            total_meetings += total
        except Exception as e:
            print(f"Error on subpage for {session_url}, subpage {subpage}: {e}")
        time.sleep(1)  # Be polite to the server
    
    return hours_sum, private_meetings, unavailable_meetings, total_meetings, dict(session_language_hours)

def write_language_csv_entry(session_url, language_hours, total_hours, private_meetings, unavailable_meetings, total_meetings):
    """
    Write language-specific hours to CSV file.
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Create file with headers if it doesn't exist
    csv_file = "un_recordings_by_language.csv"
    file_exists = os.path.exists(csv_file)
    
    with open(csv_file, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(['timestamp', 'session_url', 'language_code', 'hours', 'total_session_hours', 
                           'private_meetings', 'unavailable_meetings', 'total_meetings'])
        
        # Write a row for each language
        for lang_code, hours in language_hours.items():
            writer.writerow([timestamp, session_url, lang_code, round(hours, 4), 
                           round(total_hours, 4), private_meetings, unavailable_meetings, total_meetings])

# Main Scraper Loop
if __name__ == "__main__":
    total_pages = get_total_pages(BASE_URL)
    all_session_links = []
    for page in range(0, total_pages):
        print(f"Gathering session links from page {page+1}/{total_pages}")
        all_session_links.extend(parse_session_links(page))
        # time.sleep(1) # Be polite while gathering main page links

    print(f"Found {len(all_session_links)} sessions to process.")
    print(all_session_links)

    index = 0
    hours_sum = 0
    private_meetings = 0
    unavailable_meetings = 0
    total_meetings = 0
    lock = Lock()
    
    # Initialize CSV files
    with open("un_recordings.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["index", "url", "hours", "private_meetings", "unavailable_meetings", "total_meetings"])

    # all_session_links = ["https://conf.unog.ch/digitalrecordings/en/clients/13.0030/meetings"]

    # You can adjust max_workers based on your needs and network capacity.
    # 5 is a reasonable starting point to avoid overwhelming the server.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(process_session, url): url for url in all_session_links}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            with lock:
                index += 1
                result = future.result()
                hours_sum += result[0]
                private_meetings += result[1]
                unavailable_meetings += result[2]
                total_meetings += result[3]
                language_hours = result[4]  # New: language-specific hours
            
            try:
                # Log into the main CSV file
                with open("un_recordings.csv", "a") as f:
                    writer = csv.writer(f)
                    writer.writerow([index, url, round(result[0], 4), result[1], result[2], result[3]])
                
                # Log language-specific data to separate CSV
                write_language_csv_entry(url, language_hours, result[0], result[1], result[2], result[3])
                
            except Exception as exc:
                print(f'{url} generated an exception: {exc}')
                print(f"Total hours: {round(hours_sum, 4)}, private meetings: {private_meetings}, unavailable meetings: {unavailable_meetings}, total meetings: {total_meetings}")
    
    print(f"Total hours: {round(hours_sum, 4)}, private meetings: {private_meetings}, unavailable meetings: {unavailable_meetings}, total meetings: {total_meetings}")
    print("Scraping complete.")
    print("Language-specific data saved to: un_recordings_by_language.csv")
