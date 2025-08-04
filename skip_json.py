
import csv
import json

def find_links_to_skip(csv_file_path):
    """
    Reads a CSV file and returns a list of URLs to be skipped.

    The function identifies rows where the 'hours' column is '0.0' and extracts
    the corresponding URL from the 'url' column.

    Args:
        csv_file_path (str): The path to the input CSV file.

    Returns:
        list: A list of URLs that should be skipped.
    """
    links_to_skip = []
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        header = next(reader)  # Skip header row
        
        # Find column indices
        try:
            url_index = header.index('url')
            hours_index = header.index('hours')
        except ValueError as e:
            print(f"Error: Missing required column in CSV header - {e}")
            return []

        for row in reader:
            # Ensure row has enough columns to prevent IndexError
            if len(row) > max(url_index, hours_index):
                try:
                    # Check if hours is 0.0
                    if float(row[hours_index]) == 0.0:
                        links_to_skip.append(row[url_index])
                except (ValueError, IndexError) as e:
                    # Handle cases where conversion to float fails or row is malformed
                    print(f"Skipping malformed row: {row} - Error: {e}")
                    continue
    return links_to_skip

if __name__ == "__main__":
    csv_path = 'un_recordings.csv'
    skipped_links = find_links_to_skip(csv_path)
    
    if skipped_links:
        print("Links to be skipped (hours = 0.0):")
        for link in skipped_links:
            print(link)
    else:
        print("No links to skip or file could not be processed.")

    with open('skip_json.json', 'w') as f:
        json.dump(skipped_links, f)
