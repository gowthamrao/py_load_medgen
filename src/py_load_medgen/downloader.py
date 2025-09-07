import ftplib
import logging
from pathlib import Path
from typing import Optional

from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class Downloader:
    """
    Handles downloading data files from the NCBI FTP server with retry logic.
    """

    def __init__(self, ftp_host: str = "ftp.ncbi.nlm.nih.gov", ftp_path: str = "/pub/medgen"):
        """
        Initializes the Downloader.
        Args:
            ftp_host: The hostname of the FTP server.
            ftp_path: The base path to the MedGen files.
        """
        self.ftp_host = ftp_host
        self.ftp_path = ftp_path
        self.ftp: Optional[ftplib.FTP] = None

    def __enter__(self):
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

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def download_file(self, remote_filename: str, local_filepath: Path) -> None:
        """
        Downloads a single file from the FTP server with exponential backoff.
        Args:
            remote_filename: The name of the file on the FTP server.
            local_filepath: The local path to save the downloaded file.
        """
        if not self.ftp:
            raise ConnectionError("FTP connection not established. Use within a 'with' statement.")

        try:
            local_filepath.parent.mkdir(parents=True, exist_ok=True)
            logging.info(f"Downloading {remote_filename} to {local_filepath}...")
            with open(local_filepath, "wb") as f:
                self.ftp.retrbinary(f"RETR {remote_filename}", f.write)
            logging.info(f"Successfully downloaded {remote_filename}")
        except ftplib.all_errors as e:
            logging.error(f"FTP error while downloading {remote_filename}: {e}")
            if local_filepath.exists():
                local_filepath.unlink()
            raise
