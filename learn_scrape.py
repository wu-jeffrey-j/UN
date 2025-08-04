URL="https://conf.unog.ch/digitalrecordings/en/clients/61.0560/meetings/8DAB7464-CDD2-461C-806F-DE2CCA03F40B/10h29"

from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.firefox.options import Options

options = Options()
options.add_argument("--headless")  # Run in headless mode
options.add_argument("--disable-gpu")  # Optional but helpful
options.add_argument("--no-sandbox")  # Needed on some Linux systems
options.add_argument("--window-size=1920x1080")  # Optional: full-size rendering
driver = webdriver.Firefox(options=options)
driver.get(URL)

# Wait for JavaScript to load
driver.implicitly_wait(5)

soup = BeautifulSoup(driver.page_source, 'html.parser')
audio_time=soup.select_one("#audio-time").get_text(strip=True)
print(audio_time)

duration_str = audio_time.split("/")[-1].strip()
# Convert to hours
parts = list(map(int, duration_str.split(":")))  # [1, 34, 20]

# Pad if format is MM:SS instead of HH:MM:SS
while len(parts) < 3:
    parts.insert(0, 0)  # e.g. [0, 1, 34] becomes [0, 1, 34]

hours = parts[0] + parts[1]/60 + parts[2]/3600

print(f"Duration in hours: {hours:.4f}")

driver.quit()