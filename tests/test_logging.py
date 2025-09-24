import json
import logging
import time

from py_load_medgen.logging import JsonFormatter


def test_json_formatter_basic():
    """
    Tests that the JsonFormatter correctly formats a basic log record into a
    JSON string.
    """
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="/path/to/test.py",
        lineno=10,
        msg="This is a test message",
        args=(),
        exc_info=None,
    )

    # Simulate the time to ensure consistent timestamp output
    record.created = time.time()

    # Format the record
    formatted_json = formatter.format(record)

    # Parse the output and verify its contents
    log_object = json.loads(formatted_json)

    assert log_object["level"] == "INFO"
    assert log_object["name"] == "test_logger"
    assert log_object["message"] == "This is a test message"
    assert "timestamp" in log_object


def test_json_formatter_with_extra_fields():
    """
    Tests that the JsonFormatter includes extra fields provided in the log record.
    """
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.WARNING,
        pathname="/path/to/test.py",
        lineno=20,
        msg="Another test",
        args=(),
        exc_info=None,
    )
    # Add extra data to the record
    record.extra_field_1 = "value1"
    record.extra_field_2 = 123

    # Format the record
    formatted_json = formatter.format(record)

    # Parse the output and verify its contents
    log_object = json.loads(formatted_json)

    assert log_object["level"] == "WARNING"
    assert log_object["message"] == "Another test"
    assert "extra" in log_object
    assert log_object["extra"]["extra_field_1"] == "value1"
    assert log_object["extra"]["extra_field_2"] == 123


def test_json_formatter_with_exception():
    """
    Tests that the JsonFormatter correctly includes exception information.
    """
    formatter = JsonFormatter()
    try:
        raise ValueError("This is a test exception")
    except ValueError as e:
        # Create a log record with exception info
        record = logging.LogRecord(
            name="error_logger",
            level=logging.ERROR,
            pathname="/path/to/error.py",
            lineno=30,
            msg="An error occurred",
            args=(),
            exc_info=(type(e), e, e.__traceback__),
        )

    # Format the record
    formatted_json = formatter.format(record)

    # Parse the output and verify its contents
    log_object = json.loads(formatted_json)

    assert log_object["level"] == "ERROR"
    assert log_object["message"] == "An error occurred"
    assert "exception" in log_object
    assert "ValueError: This is a test exception" in log_object["exception"]
