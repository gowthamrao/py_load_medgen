import logging
import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest
from py_load_medgen.cli import main

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def mock_downloader():
    """Fixture to mock the Downloader and its methods."""
    with patch("py_load_medgen.cli.Downloader") as mock_downloader_cls:
        mock_downloader_instance = MagicMock()
        mock_downloader_instance.get_release_version.return_value = "2025AA"
        mock_downloader_instance.get_checksums.return_value = {
            "MRCONSO.RRF": "12345",
            "MRSTY.RRF": "67890",
        }
        # Make the context manager return the instance
        mock_downloader_cls.return_value.__enter__.return_value = (
            mock_downloader_instance
        )
        yield mock_downloader_instance


@pytest.fixture
def mock_loader():
    """Fixture to mock the LoaderFactory and the created loader."""
    with patch("py_load_medgen.cli.LoaderFactory.create_loader") as mock_create_loader:
        mock_loader_instance = MagicMock()
        # Make the context manager return the instance
        mock_create_loader.return_value.__enter__.return_value = mock_loader_instance
        yield mock_loader_instance


def run_cli(*args):
    """
    Helper function to run the CLI's main function with specified arguments,
    mocking sys.argv.
    """
    with patch.object(sys, "argv", ["py_load_medgen", *args]):
        return main()


def test_cli_graceful_exit_on_db_connection_failure(mock_downloader, caplog, tmp_path):
    """
    Tests that the CLI exits gracefully when the database connection string is invalid.
    FRD Alignment: R-4.2.1 (Error Handling)
    """
    with patch("py_load_medgen.cli.setup_logging"):
        # Create dummy files to prevent downloader from failing first
        (tmp_path / "MRCONSO.RRF").touch()
        (tmp_path / "MRSTY.RRF").touch()
        (tmp_path / "MRREL.RRF").touch()
        (tmp_path / "MRSAT.RRF").touch()
        (tmp_path / "NAMES.RRF.gz").touch()
        (tmp_path / "MedGen_HPO_Mapping.txt.gz").touch()

        # Use a DSN that is syntactically valid but points to a non-existent host
        bad_dsn = "postgresql://user:pass@nonexistent-host:5432/dbname"

        with patch("sys.exit") as mock_exit:
            run_cli("--db-dsn", bad_dsn, "--download-dir", str(tmp_path))

        # Check that sys.exit was called with a non-zero exit code
        mock_exit.assert_called_with(1)

        # Check that a specific, user-friendly error message was logged
        assert "An unexpected error occurred during the ETL process" in caplog.text


def test_cli_graceful_exit_on_download_failure(mock_downloader, caplog):
    """
    Tests that the CLI exits gracefully if the download phase fails.
    FRD Alignment: R-4.2.1 (Error Handling)
    """
    with patch("py_load_medgen.cli.setup_logging"):
        # Configure the mock downloader to raise an exception
        mock_downloader.download_file.side_effect = Exception("FTP connection failed")

        with patch("sys.exit") as mock_exit:
            run_cli("--db-dsn", "postgresql://fake", "--download-dir", ".")

        # Check that sys.exit was called with a non-zero exit code
        mock_exit.assert_called_with(1)

        # Check for a specific error message in the logs
        assert "Failed during download phase" in caplog.text


def test_cli_full_load_happy_path(
    mock_downloader, mock_loader, caplog, tmp_path
):
    """
    Tests the end-to-end "happy path" for a full load, mocking external
    dependencies (downloader, loader).
    FRD Alignment: R-2.1 (CLI), R-3.1 (Full Load)
    """
    # Arrange: Patch setup_logging to prevent it from interfering with caplog
    with patch("py_load_medgen.cli.setup_logging"):
        caplog.set_level(logging.INFO)
        # Create dummy local files that the "downloader" will "create"
        (tmp_path / "MRCONSO.RRF").touch()
        (tmp_path / "MRSTY.RRF").touch()
        (tmp_path / "MRREL.RRF").touch()
        (tmp_path / "MRSAT.RRF").touch()
        (tmp_path / "NAMES.RRF.gz").touch()
        (tmp_path / "MedGen_HPO_Mapping.txt.gz").touch()

        # Act: Run the CLI main function with appropriate arguments
        run_cli(
            "--db-dsn",
            "postgresql://user:pass@host/db",
            "--mode",
            "full",
            "--download-dir",
            str(tmp_path),
        )

        # Assert: Check for successful execution and correct calls

        # 1. Verify logging
        assert "Starting MedGen ETL run" in caplog.text
        assert "MedGen Release Version: 2025AA" in caplog.text
        assert "--- Starting ETL for MRCONSO.RRF" in caplog.text
        assert "--- ETL process finished successfully." in caplog.text

        # 2. Verify downloader was used correctly
        assert mock_downloader.download_file.call_count == 6
        mock_downloader.get_checksums.assert_called_once()

        # 3. Verify loader was used correctly
        # Check that a run was started and finished
        mock_loader.log_run_start.assert_called_once()
        mock_loader.log_run_finish.assert_called_once()
        # Check the status of the final log entry
        _, final_log_kwargs = mock_loader.log_run_finish.call_args
        assert final_log_kwargs["status"] == "Succeeded"

        # Check that staging was initialized and changes were applied for each file
        assert mock_loader.initialize_staging.call_count == 6
        assert mock_loader.bulk_load.call_count == 6
        assert mock_loader.apply_changes.call_count == 6
        # Verify the 'mode' argument in apply_changes was 'full'
        for call in mock_loader.apply_changes.call_args_list:
            assert call.kwargs["mode"] == "full"

        # 4. Verify cleanup was called
        assert mock_loader.cleanup.call_count == 6
