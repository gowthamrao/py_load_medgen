# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import json
import logging
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """
    A custom logging formatter that outputs log records as JSON strings.
    This formatter ensures that logs are structured and machine-readable,
    which is ideal for modern observability platforms.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Formats a log record into a JSON string.
        Args:
            record: The LogRecord instance to format.
        Returns:
            A JSON string representing the log record.
        """
        # Base attributes from the LogRecord
        log_object = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }

        # Include exception information if it exists
        if record.exc_info:
            log_object["exception"] = self.formatException(record.exc_info)

        # Include stack information if it exists
        if record.stack_info:
            log_object["stack_info"] = self.formatStack(record.stack_info)

        # Add any extra fields passed to the logger
        # Standard LogRecord attributes to exclude from the 'extra' dictionary
        standard_attrs = {
            "args", "asctime", "created", "exc_info", "exc_text", "filename",
            "funcName", "levelname", "levelno", "lineno", "module", "msecs",
            "message", "msg", "name", "pathname", "process", "processName",
            "relativeCreated", "stack_info", "thread", "threadName"
        }
        extra_fields = {
            key: value for key, value in record.__dict__.items()
            if key not in standard_attrs and not key.startswith('_')
        }
        if extra_fields:
            log_object["extra"] = extra_fields

        return json.dumps(log_object)
