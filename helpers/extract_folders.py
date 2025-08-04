import os
import zipfile

for folder in os.listdir('un_recordings'):
    if os.path.isdir(os.path.join('un_recordings', folder)):
        files = os.listdir(os.path.join('un_recordings', folder))
        if len(files) > 3:
            print(f"Skipping {folder} because it has more than one file")
            continue
        for file in files:
            if file.endswith('.zip'):
                try:
                    print(f"Extracting {folder}/{file}")
                    with zipfile.ZipFile(os.path.join('../un_recordings', folder, file), 'r') as zip_ref:
                        zip_ref.extractall(os.path.join('../un_recordings2', folder))
                except Exception as e:
                    print(f"Error extracting {folder}/{file}: {e}")
                    continue