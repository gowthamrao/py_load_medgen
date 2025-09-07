import ftplib
from pathlib import Path
from typing import Optional


class Downloader:
    """
    Handles downloading data files from the NCBI FTP server.
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
        self.ftp = ftplib.FTP(self.ftp_host)
        self.ftp.login()  # Anonymous login
        self.ftp.cwd(self.ftp_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the FTP connection."""
        if self.ftp:
            self.ftp.quit()

    def download_file(self, remote_filename: str, local_filepath: Path) -> None:
        """
        Downloads a single file from the FTP server.

        Args:
            remote_filename: The name of the file on the FTP server relative to the base path.
            local_filepath: The local path to save the downloaded file.
        """
        if not self.ftp:
            raise ConnectionError("FTP connection not established. Use within a 'with' statement.")

        try:
            # Ensure the parent directory for the local file exists
            local_filepath.parent.mkdir(parents=True, exist_ok=True)

            print(f"Downloading {remote_filename} to {local_filepath}...")
            with open(local_filepath, "wb") as f:
                self.ftp.retrbinary(f"RETR {remote_filename}", f.write)

            print(f"Successfully downloaded {remote_filename}")

        except ftplib.all_errors as e:
            print(f"FTP error while downloading {remote_filename}: {e}")
            # Clean up partially downloaded file on error
            if local_filepath.exists():
                local_filepath.unlink()
            raise
