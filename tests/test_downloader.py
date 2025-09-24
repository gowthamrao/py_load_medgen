# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import ftplib
from pathlib import Path
from unittest.mock import ANY, MagicMock
from builtins import ExceptionGroup

import pytest

from py_load_medgen.downloader import ChecksumsNotFoundError, Downloader


def test_downloader_flow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Tests the basic flow of the Downloader context manager and file download
    by mocking all interactions with the `ftplib`.
    """
    # 1. Arrange: Mock ftplib.FTP
    mock_ftp_instance = MagicMock()

    # This simulates the `RETR` command writing data to the file object (the callback)
    def fake_retrbinary(command, callback, rest=None):
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


def test_downloader_checksum_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Tests that the downloader correctly verifies file integrity using checksums.
    """
    # 1. Arrange: Mock ftplib.FTP
    mock_ftp_instance = MagicMock()

    # Mock retrlines to provide the fake checksum data
    def fake_retrlines(command, callback):
        # The command should be 'RETR md5sum.txt'
        if "md5sum.txt" in command:
            callback(
                "2a9d9c136c327402524c75a3e3696b4a  dummy_file.txt"
            )  # checksum for 'correct-data'
            callback("badc0d3e  other_file.txt")

    mock_ftp_instance.retrlines.side_effect = fake_retrlines

    # Mock retrbinary to provide file content
    def fake_retrbinary(command, callback, rest=None):
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
    mock_ftp_instance.retrbinary.assert_called_once()
    assert mock_ftp_instance.retrbinary.call_args[0][0] == "RETR dummy_file.txt"


    # --- Test Case 2: Failed Verification ---
    # 2. Arrange
    # Reset mocks and change retrbinary to return bad data
    mock_ftp_instance.reset_mock()
    mock_ftp_instance.retrlines.side_effect = fake_retrlines # re-apply side effect
    def fake_retrbinary_bad(command, callback, rest=None):
        if "dummy_file.txt" in command:
            callback(b'wrong-data')
    mock_ftp_instance.retrbinary.side_effect = fake_retrbinary_bad

    local_filepath_fail = tmp_path / "dummy_file.txt"

    # 3. Act & Assert
    with Downloader() as downloader:
        checksums = downloader.get_checksums("md5sum.txt")
        try:
            downloader.download_file("dummy_file.txt", local_filepath_fail, checksums)
        except ExceptionGroup as eg:
            assert len(eg.exceptions) == 1
            assert isinstance(eg.exceptions[0], ValueError)
            assert "Checksum validation failed" in str(eg.exceptions[0])
        else:
            pytest.fail("Expected an ExceptionGroup to be raised")

    # Assert that the corrupted file was cleaned up
    assert not local_filepath_fail.exists()

    # --- Test Case 3: Verification skipped if checksum for file is missing ---
    # 2. Arrange
    mock_ftp_instance.reset_mock()
    mock_ftp_instance.retrlines.side_effect = fake_retrlines # re-apply
    mock_ftp_instance.retrbinary.side_effect = fake_retrbinary # re-apply

    local_filepath_missing = tmp_path / "another_file.txt"

    # 3. Act & Assert
    with Downloader() as downloader:
        checksums = downloader.get_checksums("md5sum.txt") # checksums dict will not have 'another_file.txt'
        try:
            downloader.download_file("another_file.txt", local_filepath_missing, checksums)
        except ExceptionGroup as eg:
            assert len(eg.exceptions) == 1
            assert isinstance(eg.exceptions[0], ValueError)
            assert "No checksum found for 'another_file.txt'" in str(eg.exceptions[0])
        else:
            pytest.fail("Expected an ExceptionGroup to be raised")


def test_get_release_version(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Tests the parsing of the release version from a mocked README file.
    """
    mock_ftp_instance = MagicMock()
    monkeypatch.setattr(ftplib, "FTP", MagicMock(return_value=mock_ftp_instance))

    # --- Test Case 1: Version Found ---
    # Arrange
    def fake_retrlines_found(command, callback):
        if "README" in command:
            callback("Some header text")
            callback("Last update: September 5, 2025")
            callback("Some other text")

    mock_ftp_instance.retrlines.side_effect = fake_retrlines_found

    # Act
    with Downloader() as downloader:
        version = downloader.get_release_version()

    # Assert
    assert version == "September 5, 2025"
    mock_ftp_instance.retrlines.assert_called_once_with("RETR README", ANY)

    # --- Test Case 2: Version Not Found ---
    # Arrange
    mock_ftp_instance.reset_mock()
    def fake_retrlines_not_found(command, callback):
        if "README" in command:
            callback("Some header text")
            callback("Just some random text without a date")

    mock_ftp_instance.retrlines.side_effect = fake_retrlines_not_found

    # Act
    with Downloader() as downloader:
        version = downloader.get_release_version()

    # Assert
    assert version == "Unknown"


def test_downloader_resume_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Tests that the downloader correctly resumes a partially downloaded file.
    """
    # 1. Arrange: Mock ftplib.FTP
    mock_ftp_instance = MagicMock()

    # This simulates the `RETR` command writing the *rest* of the data
    def fake_retrbinary(command, callback, rest=None):
        callback(b"-rest-of-data")

    mock_ftp_instance.retrbinary.side_effect = fake_retrbinary
    monkeypatch.setattr(ftplib, "FTP", MagicMock(return_value=mock_ftp_instance))

    # 2. Arrange: Prepare a "partially downloaded" file
    local_filepath = tmp_path / "partial_file.txt"
    partial_content = b"initial-data"
    local_filepath.write_bytes(partial_content)
    partial_size = local_filepath.stat().st_size

    # 3. Act: Call the download method on the existing partial file
    with Downloader() as downloader:
        downloader.download_file("partial_file.txt", local_filepath)

    # 4. Assert
    # Check that retrbinary was called with the correct 'rest' offset
    mock_ftp_instance.retrbinary.assert_called_once()
    # The call looks like: retrbinary('RETR partial_file.txt', <function>, rest=<size>)
    assert mock_ftp_instance.retrbinary.call_args.kwargs["rest"] == partial_size

    # Check that the file now contains the full, combined content
    expected_content = partial_content + b"-rest-of-data"
    assert local_filepath.read_bytes() == expected_content


def test_get_checksums_raises_error_when_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Tests that get_checksums raises ChecksumsNotFoundError if the checksum
    file is not found on the FTP server.
    """
    # 1. Arrange
    mock_ftp_instance = MagicMock()
    # Simulate an FTP error (e.g., 550 File not found)
    mock_ftp_instance.retrlines.side_effect = ftplib.error_perm(
        "550 No such file or directory."
    )
    monkeypatch.setattr(ftplib, "FTP", MagicMock(return_value=mock_ftp_instance))

    # 2. Act & Assert
    with Downloader() as downloader:
        with pytest.raises(
            ChecksumsNotFoundError, match="Could not find or parse checksum file"
        ):
            downloader.get_checksums("md5sum.txt")

    # 3. Assert that the underlying FTP command was called
    mock_ftp_instance.retrlines.assert_called_once_with("RETR md5sum.txt", ANY)


def test_download_succeeds_with_no_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Tests that a download succeeds without error if no checksums are provided
    (i.e., the --no-verify flag is used).
    """
    # 1. Arrange
    mock_ftp_instance = MagicMock()

    def fake_retrbinary(command, callback, rest=None):
        callback(b'some-data')

    mock_ftp_instance.retrbinary.side_effect = fake_retrbinary
    monkeypatch.setattr(ftplib, "FTP", MagicMock(return_value=mock_ftp_instance))

    local_filepath = tmp_path / "unverified_file.txt"

    # 2. Act
    # No exception should be raised here.
    with Downloader() as downloader:
        downloader.download_file("unverified_file.txt", local_filepath, checksums=None)

    # 3. Assert
    # The file should exist and have the downloaded content.
    assert local_filepath.exists()
    assert local_filepath.read_text() == "some-data"
    # Crucially, verify_file should NOT have been called. We can test this by
    # mocking it on the downloader instance.
    with Downloader() as downloader:
        downloader.verify_file = MagicMock()
        downloader.download_file("unverified_file.txt", local_filepath, checksums=None)
        downloader.verify_file.assert_not_called()
