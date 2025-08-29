from pathlib import Path
from typing import Union

from pydantic import BaseModel, Field

from src.logger import LoggerType
from src.schema import MigrationInfo
from src.utils import (
    compare_migration_folder_name_with_version,
    is_valid_migration_directory,
)


class MigrationNode(BaseModel):
    previous: Union["MigrationNode", None] = Field(default=None)
    next: Union["MigrationNode", None] = Field(default=None)
    info: MigrationInfo


class MigrationChain:
    def __init__(self, migrations_dir: Path, logger: LoggerType):
        self.head: MigrationNode | None = None
        self.tail: MigrationNode | None = None
        self._is_initialized = False
        self._versions_dir = migrations_dir
        self._logger = logger

    def _find_first_migration(
        self, migration_dirs: list[Path]
    ) -> MigrationNode | None:
        for d in migration_dirs:
            info_file = d / "info.json"
            migration_info = MigrationInfo.model_validate_json(
                info_file.read_text()
            )
            if not migration_info.previous_version:
                return MigrationNode(
                    info=migration_info,
                )
        return None

    def build_list(self):
        if self._is_initialized:
            return

        migration_dirs = list(
            filter(
                lambda d: is_valid_migration_directory(d, self._logger),
                self._versions_dir.iterdir(),
            )
        )
        current_migration = self._find_first_migration(migration_dirs)
        if not current_migration:
            self._logger.warn("No migrations found")
            return

        self.head = current_migration

        while current_migration.info.next_version:
            found_next = False
            for d in migration_dirs:
                if not compare_migration_folder_name_with_version(
                    version=current_migration.info.next_version,
                    full_folder_name=d.name,
                ):
                    continue
                info_file = d / "info.json"
                migration_info = MigrationInfo.model_validate_json(
                    info_file.read_text()
                )
                current_migration.next = MigrationNode(
                    previous=current_migration,
                    next=None,
                    info=migration_info,
                )
                current_migration = current_migration.next
                found_next = True
                break

            if not found_next:
                self._logger.error(
                    f"Migration chain broken: next_version {current_migration.info.next_version} not found"
                )
                break
        self.tail = current_migration
        self._is_initialized = True

    def append(self, node: MigrationNode) -> None:
        if not self._is_initialized:
            self.build_list()

        if self.head is None:
            self.head = node
            return

        current = self.head
        while current.next:
            current = current.next

        current.next = node
        node.previous = current

    def find_by_version(self, version: str) -> MigrationNode | None:
        if not self._is_initialized:
            self.build_list()

        current = self.head
        while current:
            if current.info.version == version:
                return current
            current = current.next
        return None
