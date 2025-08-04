import os
import shutil

def copy_mp3s_with_structure(src_root, dst_root):
    for root, dirs, files in os.walk(src_root):
        for file in files:
            if file.lower().endswith(".mp3"):
                # Full path to original file
                src_path = os.path.join(root, file)

                # Relative path from src_root
                rel_path = os.path.relpath(src_path, src_root)

                # Full destination path
                dst_path = os.path.join(dst_root, rel_path)

                # Create destination directory if needed
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)

                # Copy the file
                shutil.copy2(src_path, dst_path)
                print(f"Copied: {rel_path}")

# Example usage
copy_mp3s_with_structure("../un_recordings", "../un_recordings2")
