import tomllib
import typing
from pathlib import Path
from typing import Literal

from asynch import Connection

from src.schema import MigrationConfig

if typing.TYPE_CHECKING:
    from src.migration_chain import MigrationNode


from src.logger import LoggerType


async def run_migration(
    versions: list["MigrationNode"],
    versions_dir: Path,
    mode: Literal["upgrade", "downgrade"],
    connection: Connection,
    logger: LoggerType,
) -> None:
    logger.info(
        "run-migration", mode=mode, versions=[v.info.version for v in versions]
    )

    for migration_node in versions:
        migration_dir = None
        for d in versions_dir.iterdir():
            if compare_migration_folder_name_with_version(
                version=migration_node.info.version,
                full_folder_name=d.name,
            ):
                migration_dir = d
                break

        if not migration_dir:
            logger.error(
                "run-migration",
                error="Migration directory not found",
                version=migration_node.info.version,
                exc_info=True,
            )
            return

        sql_file = migration_dir / f"{mode}.sql"
        if not sql_file.is_file():
            logger.error(
                "run-migration",
                error=f"No {mode} SQL file found",
                version=migration_node.info.version,
                exc_info=True,
            )
            return

        logger.info(
            "run-migration",
            mode=mode,
            version=migration_node.info.version,
            target=migration_dir.name,
        )

        sql_content = sql_file.read_text().strip()
        if not sql_content:
            logger.error(
                "run-migration",
                error=f"Empty {mode} SQL file",
                version=migration_node.info.version,
                file=str(sql_file),
                exc_info=True,
            )
            return

        statements = [
            stmt.strip() for stmt in sql_content.split(";") if stmt.strip()
        ]

        if not statements:
            logger.error(
                "run-migration",
                error=f"No valid SQL statements in {mode} file",
                version=migration_node.info.version,
                file=str(sql_file),
                exc_info=True,
            )
            return

        if not await execute_sql_statements(
            connection, statements, migration_node.info.version, logger
        ):
            return

        if mode == "upgrade":
            if not await update_migration_version(
                connection, migration_node.info.version, logger
            ):
                return
        else:  # downgrade
            # For downgrade, update to previous version (or empty string for base)
            previous_version = migration_node.info.previous_version or ""
            if not await update_migration_version(
                connection, previous_version, logger
            ):
                return


def is_valid_migration_directory(path: Path, logger: LoggerType) -> bool:
    if not path.is_dir():
        logger.warn("Migration directory is not a directory", dir=str(path))
        return False

    if "--" not in path.name:
        logger.warn(
            "Migration directory name does not contain '--'", dir=str(path)
        )
        return False

    info_file = path / "info.json"

    if not info_file.is_file():
        logger.warn("Migration directory is missing info.json", dir=str(path))
        return False

    upgrade_file = path / "upgrade.sql"
    downgrade_file = path / "downgrade.sql"

    if not upgrade_file.is_file() or not downgrade_file.is_file():
        logger.warn(
            "Migration directory is missing upgrade or downgrade file",
            dir=str(path),
        )
        return False

    return True


def compare_migration_folder_name_with_version(
    *, version: str, full_folder_name: str
):
    split_name = full_folder_name.split("--")
    return split_name[0] == version


def load_config(config_path: str) -> MigrationConfig:
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_file, "rb") as f:
        toml_data = tomllib.load(f)

    return MigrationConfig.model_validate(toml_data)


async def create_migratino_table(connection: Connection, logger: LoggerType):
    try:
        async with connection.cursor() as cursor:
            await cursor.execute("""CREATE TABLE IF NOT EXISTS ch_migrations
(
    version String
)
ENGINE = TinyLog;
""")
    except Exception as e:
        logger.error(
            "create-migration-table",
            error="Failed to create migration table",
            details=str(e),
            exc_info=True,
        )


async def get_current_db_migration_version(
    connection: Connection,
    logger: LoggerType,
) -> str | None:
    try:
        async with connection.cursor() as cursor:
            await cursor.execute("select version from ch_migrations;")
            ret = await cursor.fetchone()
            if not ret:
                return ""
            return ret[0]
    except Exception as e:
        logger.error(
            "get-current-version",
            error="Failed to get current migration version",
            details=str(e),
            exc_info=True,
        )
        return None


async def update_migration_version(
    connection: Connection, version: str, logger: LoggerType
) -> bool:
    try:
        async with connection.cursor() as cursor:
            await cursor.execute("TRUNCATE TABLE ch_migrations;")
            await cursor.execute(
                "INSERT INTO ch_migrations (version) VALUES", [(version,)]
            )
            logger.info(
                "update-migration-version",
                message="Updated migration table",
                version=version,
            )
            return True
    except Exception as e:
        logger.error(
            "update-migration-version",
            error="Failed to update migration table",
            version=version,
            details=str(e),
            exc_info=True,
        )
    return False


async def execute_sql_statements(
    connection: Connection,
    statements: list[str],
    migration_version: str,
    logger: LoggerType,
) -> bool:
    async with connection.cursor() as cursor:
        for i, statement in enumerate(statements, 1):
            logger.debug(
                "execute-sql",
                version=migration_version,
                statement=i,
                total=len(statements),
            )
            try:
                await cursor.execute(statement)
            except Exception as e:
                logger.error(
                    "execute-sql",
                    error="SQL execution failed",
                    version=migration_version,
                    statement=statement,
                    details=str(e),
                    exc_info=True,
                )
                return False
            else:
                return True
    return False
