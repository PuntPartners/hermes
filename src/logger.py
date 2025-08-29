import json
import logging
import os
from logging.handlers import TimedRotatingFileHandler

import structlog
from structlog.processors import CallsiteParameter

LoggerType = structlog.stdlib.BoundLogger


class UnstructuredLoggingFormatter(logging.Formatter):
    def format(self, record):
        try:
            data = json.loads(record.getMessage())
        except json.JSONDecodeError:
            data = {"message": record.getMessage()}

        message_parts = [data.pop("level", record.levelname).upper()]
        for k, v in data.items():
            if v is None:
                continue
            if k in ("func_name", "event", "timestamp"):
                message_parts.append(v)
            elif k in ("exception", "error"):
                if isinstance(v, Exception):
                    message_parts.append(f"{v.__class__.__name__}: {v!s}")
                else:
                    message_parts.append(v)
            else:
                message_parts.append(f"{k}={v!r}")

        if record.exc_info:
            message_parts.append(self.formatException(record.exc_info))

        return " | ".join(message_parts)


def setup_logging(filename: str | None = None, level: int | None = None) -> None:
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(UnstructuredLoggingFormatter())

    handlers = [console_handler]

    if filename is not None:
        file_handler = TimedRotatingFileHandler(
            filename, when="midnight", interval=1, backupCount=30
        )
        file_handler.setFormatter(logging.Formatter("%(message)s"))
        handlers.append(file_handler)

    logging.basicConfig(
        level=level or os.environ.get("LOG_LEVEL", logging.INFO),
        format="%(message)s",
        handlers=handlers,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.format_exc_info,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.CallsiteParameterAdder(
                parameters=[CallsiteParameter.FUNC_NAME]
            ),
            structlog.stdlib.filter_by_level,
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
