#!/usr/bin/env python3
"""
Script to read un_recordings_by_language.csv and output total hours for each 2-letter language code.
"""

import csv
import os
from collections import defaultdict
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def is_two_letter_code(language_code):
    """
    Check if a language code is exactly 2 letters.
    
    Args:
        language_code: Language code string
        
    Returns:
        bool: True if exactly 2 letters, False otherwise
    """
    return len(language_code) == 2 and language_code.isalpha()

def analyze_language_hours(csv_file="un_recordings_by_language.csv"):
    """
    Read the CSV file and analyze hours per 2-letter language code.
    
    Args:
        csv_file: Path to the CSV file
        
    Returns:
        dict: Dictionary with language codes as keys and total hours as values
    """
    if not os.path.exists(csv_file):
        logger.error(f"❌ CSV file {csv_file} does not exist")
        return {}
    
    language_hours = defaultdict(float)
    total_rows = 0
    processed_rows = 0
    skipped_rows = 0
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                total_rows += 1
                language_code = row.get('language_code', '').strip()
                hours_str = row.get('hours', '0')
                
                # Skip if language code is not 2 letters
                if not is_two_letter_code(language_code):
                    skipped_rows += 1
                    logger.debug(f"Skipping non-2-letter language code: {language_code}")
                    continue
                
                try:
                    hours = float(hours_str)
                    language_hours[language_code] += hours
                    processed_rows += 1
                except ValueError:
                    logger.warning(f"⚠️ Invalid hours value for {language_code}: {hours_str}")
                    skipped_rows += 1
    
    except Exception as e:
        logger.error(f"❌ Error reading CSV file: {e}")
        return {}
    
    logger.info(f"📊 Processed {processed_rows} rows, skipped {skipped_rows} rows out of {total_rows} total")
    return dict(language_hours)

def print_language_summary(language_hours):
    """
    Print a summary of hours per language code.
    
    Args:
        language_hours: Dictionary with language codes and total hours
    """
    if not language_hours:
        logger.warning("⚠️ No 2-letter language codes found")
        return
    
    logger.info("📊 LANGUAGE HOURS SUMMARY")
    logger.info("=" * 40)
    
    # Sort by hours (descending)
    sorted_languages = sorted(language_hours.items(), key=lambda x: x[1], reverse=True)
    
    total_hours = sum(language_hours.values())
    
    for language_code, hours in sorted_languages:
        percentage = (hours / total_hours * 100) if total_hours > 0 else 0
        logger.info(f"{language_code}: {hours:.2f} hours ({percentage:.1f}%)")
    
    logger.info("=" * 40)
    logger.info(f"TOTAL: {total_hours:.2f} hours across {len(language_hours)} languages")

def save_language_summary(language_hours, output_file="language_hours_summary.csv"):
    """
    Save the language hours summary to a CSV file.
    
    Args:
        language_hours: Dictionary with language codes and total hours
        output_file: Output CSV file path
    """
    if not language_hours:
        logger.warning("⚠️ No data to save")
        return
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['language_code', 'total_hours'])
            
            # Sort by hours (descending)
            sorted_languages = sorted(language_hours.items(), key=lambda x: x[1], reverse=True)
            
            for language_code, hours in sorted_languages:
                writer.writerow([language_code, round(hours, 4)])
        
        logger.info(f"💾 Summary saved to: {output_file}")
        
    except Exception as e:
        logger.error(f"❌ Error saving summary: {e}")

def main():
    """
    Main function to run the language analysis.
    """
    logger.info("🎬 Starting language hours analysis")
    
    # Analyze the CSV file
    language_hours = analyze_language_hours()
    
    if not language_hours:
        logger.error("❌ No valid language data found")
        return
    
    # Print summary
    print_language_summary(language_hours)
    
    # Save summary to CSV
    save_language_summary(language_hours)
    
    logger.info("✅ Analysis complete!")

if __name__ == "__main__":
    main()
