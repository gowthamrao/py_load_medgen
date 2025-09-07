import ftplib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from py_load_medgen.downloader import Downloader


def test_downloader_flow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    Tests the basic flow of the Downloader context manager and file download
    by mocking all interactions with the `ftplib`.
    """
    # 1. Arrange: Mock ftplib.FTP
    mock_ftp_instance = MagicMock()

    # This simulates the `RETR` command writing data to the file object (the callback)
    def fake_retrbinary(command, callback):
        callback(b'dummy-data')

    mock_ftp_instance.retrbinary.side_effect = fake_retrbinary

    # The `ftplib.FTP` call returns the mocked instance
    monkeypatch.setattr(ftplib, "FTP", MagicMock(return_value=mock_ftp_instance))

    # 2. Arrange: Prepare paths and downloader instance
    local_filepath = tmp_path / "test.gz"

    # 3. Act: Use the downloader within a context manager
    with Downloader() as downloader:
        downloader.download_file("test.gz", local_filepath)

    # 4. Assert
    # Check that the FTP connection was managed correctly
    ftplib.FTP.assert_called_once_with("ftp.ncbi.nlm.nih.gov")
    mock_ftp_instance.login.assert_called_once()
    mock_ftp_instance.cwd.assert_called_once_with("/pub/medgen")
    mock_ftp_instance.quit.assert_called_once()

    # Check that the download command was issued. We check the command and that the
    # callback is a callable function.
    mock_ftp_instance.retrbinary.assert_called_once()
    assert mock_ftp_instance.retrbinary.call_args[0][0] == "RETR test.gz"
    assert callable(mock_ftp_instance.retrbinary.call_args[0][1])

    # Check that the file was created with the dummy data from the callback
    assert local_filepath.exists()
    assert local_filepath.read_text() == "dummy-data"
