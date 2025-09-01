import logging

import pytest
import structlog

from src.logger import setup_logging
from src.tests.fixtures import *  # noqa:  F403


def pytest_configure(config):
    setup_logging(
        filename=None,
        level=logging.WARNING,
        log_to_file=False,
        log_to_stream=True,
    )


@pytest.fixture(autouse=True)
def reset_structlog():
    structlog.reset_defaults()
    structlog.configure(
        processors=[
            structlog.testing.LogCapture(),
        ],
        cache_logger_on_first_use=False,
    )
