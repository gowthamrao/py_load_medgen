# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import ftplib
import hashlib
import logging
import re
from pathlib import Path
from typing import Optional, Self

from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ChecksumsNotFoundError(Exception):
    """Custom exception raised when the checksum file is not found on the FTP server."""

    pass


class Downloader:
    """
    Handles downloading data files from the NCBI FTP server with retry logic.
    """

    def __init__(
        self, ftp_host: str = "ftp.ncbi.nlm.nih.gov", ftp_path: str = "/pub/medgen"
    ):
        """
        Initializes the Downloader.
        Args:
            ftp_host: The hostname of the FTP server.
            ftp_path: The base path to the MedGen files.
        """
        self.ftp_host = ftp_host
        self.ftp_path = ftp_path
        self.ftp: Optional[ftplib.FTP] = None

    def __enter__(self) -> Self:
        """Establish the FTP connection."""
        logging.info(f"Connecting to FTP server: {self.ftp_host}")
        self.ftp = ftplib.FTP(self.ftp_host)
        self.ftp.login()  # Anonymous login
        self.ftp.cwd(self.ftp_path)
        logging.info(f"Successfully connected and changed directory to {self.ftp_path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the FTP connection."""
        if self.ftp:
            self.ftp.quit()
            logging.info("FTP connection closed.")

    def list_files(self) -> list[str]:
        """Lists files in the current FTP directory."""
        if not self.ftp:
            raise ConnectionError("FTP connection not established.")
        return self.ftp.nlst()

    def get_checksums(self, checksum_filename: str = "md5sum.txt") -> dict[str, str]:
        """
        Downloads and parses the checksum file.
        Args:
            checksum_filename: The name of the checksum file on the FTP server.
        Returns:
            A dictionary mapping filenames to their expected MD5 checksums.
        """
        if not self.ftp:
            raise ConnectionError("FTP connection not established.")

        checksums: dict[str, str] = {}
        try:
            lines: list[str] = []
            self.ftp.retrlines(f"RETR {checksum_filename}", lines.append)
            for line in lines:
                # Format is typically: <checksum>  <filename>
                parts = line.split()
                if len(parts) == 2:
                    checksum, filename = parts
                    # The filenames in md5sum.txt might have a './' prefix
                    checksums[filename.lstrip("./")] = checksum
            return checksums
        except ftplib.all_errors as e:
            raise ChecksumsNotFoundError(
                f"Could not find or parse checksum file '{checksum_filename}' on the FTP server. "
                f"To proceed without verification, use the --no-verify flag. Original error: {e}"
            ) from e

    def get_release_version(self, readme_filename: str = "README") -> str:
        """
        Downloads the README file and parses it to find the release date/version.
        Args:
            readme_filename: The name of the README file on the FTP server.
        Returns:
            The release version string (e.g., a date) or "Unknown" if not found.
        """
        if not self.ftp:
            raise ConnectionError("FTP connection not established.")

        logging.info(f"Attempting to find release version from '{readme_filename}'...")
        lines: list[str] = []
        try:
            self.ftp.retrlines(f"RETR {readme_filename}", lines.append)
            for line in lines:
                # Common patterns for release dates in README files
                match = re.search(
                    r"(?:Last update|Release Date|Version):\s*(.*)",
                    line,
                    re.IGNORECASE,
                )
                if match:
                    version = match.group(1).strip()
                    logging.info(f"Found release version: {version}")
                    return version

            logging.warning("Release version not found in README. Returning 'Unknown'.")
            return "Unknown"
        except ftplib.all_errors as e:
            logging.warning(f"Could not download or parse '{readme_filename}': {e}")
            return "Unknown"

    @staticmethod
    def _calculate_md5(filepath: Path) -> str:
        """Calculates the MD5 checksum of a local file."""
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def verify_file(self, local_filepath: Path, checksums: dict[str, str]) -> bool:
        """
        Verifies the integrity of a downloaded file using its MD5 checksum.
        Args:
            local_filepath: The path to the local file.
            checksums: A dictionary of filenames and their expected checksums.
        Returns:
            True if the file is valid, False otherwise.
        """
        filename = local_filepath.name
        if filename not in checksums:
            # This case should ideally not be hit if get_checksums is successful
            # and the file is in the manifest.
            raise ValueError(f"No checksum found for '{filename}' in the provided checksums dictionary.")

        expected_md5 = checksums[filename]
        logging.info(f"Verifying checksum for {filename}...")
        actual_md5 = self._calculate_md5(local_filepath)

        if actual_md5 == expected_md5:
            logging.info(f"Checksum valid for {filename}.")
            return True
        else:
            logging.error(
                f"Checksum mismatch for {filename}! "
                f"Expected: {expected_md5}, Got: {actual_md5}"
            )
            return False

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def download_file(
        self,
        remote_filename: str,
        local_filepath: Path,
        checksums: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Downloads a single file from the FTP server, resuming if partially downloaded,
        and verifies its checksum upon completion.

        Args:
            remote_filename: The name of the file on the FTP server.
            local_filepath: The local path to save the downloaded file.
            checksums: A dictionary of checksums to verify against. If None,
            verification is skipped.
        """
        if not self.ftp:
            raise ConnectionError(
                "FTP connection not established. Use within a 'with' statement."
            )

        try:
            local_filepath.parent.mkdir(parents=True, exist_ok=True)

            # --- Resumption Logic ---
            rest_pos = 0
            open_mode = "wb"
            if local_filepath.exists():
                rest_pos = local_filepath.stat().st_size
                open_mode = "ab"

            if rest_pos > 0:
                logging.info(
                    f"Resuming download for {remote_filename} from byte {rest_pos}."
                )
            else:
                logging.info(f"Downloading {remote_filename} to {local_filepath}...")

            # The 'rest' argument tells retrbinary where to start the download.
            with open(local_filepath, open_mode) as f:
                # The `rest` parameter is passed to the underlying `sendcmd`, so it
                # should only be provided when we are actually resuming.
                self.ftp.retrbinary(
                    f"RETR {remote_filename}",
                    f.write,
                    rest=rest_pos if rest_pos > 0 else None,
                )

            logging.info(f"Successfully downloaded {remote_filename}")

            if checksums is not None:
                if not self.verify_file(local_filepath, checksums):
                    # If checksum fails, the file is corrupt.
                    # Delete it for a clean retry.
                    local_filepath.unlink()
                    raise ValueError(
                        f"Checksum validation failed for {remote_filename}"
                    )

        except* ftplib.all_errors as eg:
            for e in eg.exceptions:
                logging.error(f"FTP error during download of {remote_filename}: {e}")
            # We leave the partial file in place for the next retry attempt.
            raise
        except* ValueError as eg:
            for e in eg.exceptions:
                logging.error(
                    f"Checksum validation failed for {remote_filename}: {e}"
                )
            # If it's a checksum error, the file is already deleted.
            raise
