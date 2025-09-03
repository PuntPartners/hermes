import time
from dataclasses import dataclass
from typing import Generator

import pytest
import structlog
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.core.waiting_utils import wait_for_logs

from src.env import Settings
from src.logger import LoggerType


@dataclass
class ClickHouseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def connection_url(self) -> str:
        return f"clickhouse://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@pytest.fixture(scope="session")
@pytest.mark.integration
def clickhouse_container_config() -> Generator[ClickHouseConfig, None, None]:
    clickhouse = ClickHouseContainer(
        "clickhouse/clickhouse-server:25.4",
        username="test",
        password="test",
        dbname="test",
    )

    try:
        clickhouse.start()

        wait_for_logs(
            container=clickhouse,
            predicate=".*Logging errors to.*",
            timeout=60,
        )

        status = 1
        retries = 10
        while status and retries > 0:
            status, _ = clickhouse.exec([
                "clickhouse-client",
                "--host",
                "localhost",
                "--port",
                "9000",
                "--query",
                "SELECT 1",
            ])
            if status:
                time.sleep(2)
                retries -= 1
            else:
                break

        if status:
            raise RuntimeError(
                "Failed to connect to ClickHouse after multiple retries"
            )

        yield ClickHouseConfig(
            host=clickhouse.get_container_host_ip(),
            port=clickhouse.get_exposed_port(9000),
            database="default",
            user="test",
            password="test",
        )

    finally:
        clickhouse.stop()


@pytest.fixture
def test_settings(clickhouse_container_config: ClickHouseConfig) -> Settings:
    return Settings(
        CLICKHOUSE_HOST=clickhouse_container_config.host,
        CLICKHOUSE_PORT=clickhouse_container_config.port,
        CLICKHOUSE_DATABASE=clickhouse_container_config.database,
        CLICKHOUSE_USER=clickhouse_container_config.user,
        CLICKHOUSE_PASSWORD=clickhouse_container_config.password,
    )


@pytest.fixture
def test_logger() -> LoggerType:
    return structlog.get_logger()
