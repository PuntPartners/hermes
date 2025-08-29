import logging
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field


class MigrationInfo(BaseModel):
    message: str
    version: str = Field()
    previous_version: str | None
    next_version: str | None
    creation_date: str


class MigrationConfig(BaseModel):
    migrations_location: str | Path = Field(
        default="versions", alias="migrations-location"
    )
    log_level: Literal["debug", "info", "warning", "error", "critical"] = Field(
        default="info"
    )

    @property
    def get_migration_dir(self) -> Path:
        path = Path(self.migrations_location)
        if not path.exists():
            path.mkdir()
        return path

    @property
    def get_log_level(self):
        level_map = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }
        return level_map[self.log_level]
