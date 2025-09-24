# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import logging
from pathlib import Path

from py_load_medgen.downloader import Downloader

# Suppress verbose logging from the downloader for this one-off script
logging.basicConfig(level=logging.WARNING)

def fetch_readme():
    """
    Uses the project's Downloader to fetch the README from the MedGen FTP server.
    """
    download_dir = Path(".")
    readme_local_path = download_dir / "FTP_README.txt"

    print("Attempting to download README from FTP server...")

    try:
        with Downloader() as downloader:
            files = downloader.list_files()
            readme_filename = None
            # Find the main README, not specific ones like README.md
            # The main README often has no extension or a .txt extension.
            # Based on FTP listings, it's often just 'README'.
            potential_names = ['README', 'README.txt']
            for f in files:
                if f in potential_names:
                    readme_filename = f
                    break

            if not readme_filename:
                # Fallback to find any file named readme as a last resort
                for f in files:
                    if 'readme' in f.lower():
                        readme_filename = f
                        break

            if not readme_filename:
                print("Error: Could not find a suitable README file on the FTP server.")
                return

            print(f"Found README file: '{readme_filename}'. Downloading...")
            # We don't have checksums for the README, so pass None
            downloader.download_file(readme_filename, readme_local_path, checksums=None)

            if readme_local_path.exists():
                print(
                    f"Successfully downloaded '{readme_filename}' "
                    f"to '{readme_local_path}'"
                )
            else:
                print(f"Error: Download of '{readme_filename}' failed.")

    except Exception as e:
        import traceback
        print(f"An error occurred: {e}")
        print(traceback.format_exc())

if __name__ == "__main__":
    fetch_readme()