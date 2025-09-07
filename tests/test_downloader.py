import ftplib
from pathlib import Path
from unittest.mock import MagicMock, ANY

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


def test_downloader_checksum_verification(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    Tests that the downloader correctly verifies file integrity using checksums.
    """
    # 1. Arrange: Mock ftplib.FTP
    mock_ftp_instance = MagicMock()

    # Mock retrlines to provide the fake checksum data
    def fake_retrlines(command, callback):
        # The command should be 'RETR md5sum.txt'
        if "md5sum.txt" in command:
            callback("2a9d9c136c327402524c75a3e3696b4a  dummy_file.txt") # checksum for 'correct-data'
            callback("badc0d3e  other_file.txt")

    mock_ftp_instance.retrlines.side_effect = fake_retrlines

    # Mock retrbinary to provide file content
    def fake_retrbinary(command, callback):
        if "dummy_file.txt" in command:
            callback(b'correct-data')

    mock_ftp_instance.retrbinary.side_effect = fake_retrbinary
    monkeypatch.setattr(ftplib, "FTP", MagicMock(return_value=mock_ftp_instance))

    # --- Test Case 1: Successful Download and Verification ---
    # 2. Arrange
    local_filepath_success = tmp_path / "dummy_file.txt"

    # 3. Act
    with Downloader() as downloader:
        checksums = downloader.get_checksums("md5sum.txt")
        downloader.download_file("dummy_file.txt", local_filepath_success, checksums)

    # 4. Assert
    assert local_filepath_success.exists()
    assert local_filepath_success.read_text() == "correct-data"
    mock_ftp_instance.retrlines.assert_called_once_with("RETR md5sum.txt", ANY)
    mock_ftp_instance.retrbinary.assert_called_once_with("RETR dummy_file.txt", ANY)


    # --- Test Case 2: Failed Verification ---
    # 2. Arrange
    # Reset mocks and change retrbinary to return bad data
    mock_ftp_instance.reset_mock()
    mock_ftp_instance.retrlines.side_effect = fake_retrlines # re-apply side effect
    def fake_retrbinary_bad(command, callback):
        if "dummy_file.txt" in command:
            callback(b'wrong-data')
    mock_ftp_instance.retrbinary.side_effect = fake_retrbinary_bad

    local_filepath_fail = tmp_path / "dummy_file.txt"

    # 3. Act & Assert
    with Downloader() as downloader:
        checksums = downloader.get_checksums("md5sum.txt")
        with pytest.raises(ValueError, match="Checksum validation failed"):
            downloader.download_file("dummy_file.txt", local_filepath_fail, checksums)

    # Assert that the corrupted file was cleaned up
    assert not local_filepath_fail.exists()
