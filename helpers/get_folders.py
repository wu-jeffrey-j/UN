import os

def get_folders(path):
    return [f for f in os.listdir(path) if os.path.isdir(os.path.join(path, f))]

folders = get_folders("../un_recordings2")

with open("folders.txt", "w") as f:
    for folder in folders:
        f.write(folder + "\n")