from pydub import AudioSegment
import os
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

def mix_audio_pair(original_path, translation_path, output_file, translation_boost_dB=4, original_reduce_dB=10):
    original = AudioSegment.from_file(original_path)
    translation = AudioSegment.from_file(translation_path)

    # Ensure same duration by padding shorter one with silence
    max_duration = max(len(original), len(translation))
    original = original + AudioSegment.silent(duration=max_duration - len(original))
    translation = translation + AudioSegment.silent(duration=max_duration - len(translation))

    # Adjust volumes
    original = original - original_reduce_dB  # make original quieter
    translation = translation + translation_boost_dB  # make translation louder

    print(f"Original: {original_path}, Translation: {translation_path}")

    # Mix the two together
    mixed = translation.overlay(original)

    # Normalize the mixed audio
    mixed = mixed.normalize()

    mixed.export(output_file, format="mp3")
    print(f"Exported: {output_file}")

def mix_audio_session(session_dir, input_dir, output_dir):
    original = os.path.join(input_dir, session_dir, "ORIGINAL.mp3")
    translations = []
    for filename in os.listdir(os.path.join(input_dir, session_dir)):
        if filename.endswith(".mp3") and filename != "ORIGINAL.mp3":
            translations.append(filename)

    output_dir = os.path.join(output_dir, session_dir)
    os.makedirs(output_dir, exist_ok=True)

    translations = [os.path.join(input_dir, session_dir, filename) for filename in translations]

    with ThreadPoolExecutor(max_workers=5) as executor:
        for translation_path in translations:
            output_file = os.path.join(output_dir, translation_path.split("/")[-1])
            executor.submit(mix_audio_pair, original, translation_path, output_file)

if __name__ == "__main__":
    input_dir = "un_recordings2"
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        for session_dir in os.listdir(input_dir):
            if not os.path.isdir(os.path.join(input_dir, session_dir)):
                continue
            executor.submit(mix_audio_session, session_dir, input_dir, output_dir)
    print("Mixing complete.")

