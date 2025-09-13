import logging
import os
from unittest.mock import MagicMock, patch

import pytest

from py_load_medgen.cli import main, setup_logging
from py_load_medgen.logging import JsonFormatter


@pytest.fixture
def mock_argparse():
    """Fixture to mock argparse and avoid parsing sys.argv."""
    with patch("py_load_medgen.cli.argparse.ArgumentParser") as mock_parser:
        # Mock the parser instance and its parse_args method
        mock_instance = mock_parser.return_value
        mock_args = MagicMock()
        mock_args.db_dsn = "mock_dsn"
        mock_args.mode = "full"
        mock_args.download_dir = "/tmp"
        mock_args.no_verify = False
        mock_args.max_parse_errors = 100
        mock_instance.parse_args.return_value = mock_args
        yield mock_instance


@pytest.fixture
def mock_downloader():
    """Fixture to mock the Downloader."""
    with patch("py_load_medgen.cli.Downloader") as mock:
        yield mock


@pytest.fixture
def mock_loader():
    """Fixture to mock the LoaderFactory and the created loader."""
    with patch("py_load_medgen.cli.LoaderFactory.create_loader") as mock:
        yield mock


def test_setup_logging_json(monkeypatch):
    """
    Tests that setup_logging configures a JsonFormatter when
    LOG_FORMAT is set to 'json'.
    """
    monkeypatch.setenv("LOG_FORMAT", "json")
    setup_logging()

    root_logger = logging.getLogger()
    # The root logger should have exactly one handler
    assert len(root_logger.handlers) == 1
    handler = root_logger.handlers[0]
    # The handler's formatter should be an instance of JsonFormatter
    assert isinstance(handler.formatter, JsonFormatter)

    # Clean up by resetting the environment variable and logger
    monkeypatch.delenv("LOG_FORMAT")
    setup_logging()  # Reset to default
    assert not isinstance(logging.getLogger().handlers[0].formatter, JsonFormatter)


def test_setup_logging_text_default():
    """
    Tests that setup_logging defaults to standard text formatting
    when LOG_FORMAT is not set.
    """
    # Ensure the environment variable is not set
    if "LOG_FORMAT" in os.environ:
        del os.environ["LOG_FORMAT"]

    setup_logging()
    root_logger = logging.getLogger()
    assert len(root_logger.handlers) == 1
    handler = root_logger.handlers[0]
    # The formatter should NOT be a JsonFormatter
    assert not isinstance(handler.formatter, JsonFormatter)
    # It should be a standard Formatter
    assert isinstance(handler.formatter, logging.Formatter)
