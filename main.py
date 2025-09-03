import uuid
from datetime import datetime
from pathlib import Path
from typing import Annotated, Optional, TypedDict, cast

import structlog
import toml
from asynch import Connection
from cyclopts import App, Parameter

from src.env import settings
from src.logger import LoggerType, setup_logging
from src.migration_chain import MigrationChain
from src.schema import MigrationConfig, MigrationInfo
from src.utils import (
    compare_migration_folder_name_with_version,
    create_migratino_table,
    get_current_db_migration_version,
    is_valid_migration_directory,
    load_config,
    run_migration,
)

app = App(name="hermes")


class MigrationContext(TypedDict):
    logger: LoggerType
    migration_location: Path
    migration_list: MigrationChain
    current_version: str


async def initialize_migration_context(
    config_path: str, operation: str
) -> MigrationContext | None:
    config = load_config(config_path)
    setup_logging(
        filename=str(config.log_file_path) if config.log_to_file else None,
        level=config.get_log_level,
        log_to_file=config.log_to_file,
        log_to_stream=config.log_to_stream,
    )
    logger: LoggerType = structlog.get_logger()

    migration_location = cast(Path, config.get_migration_dir)
    logger.info(
        operation,
        migration_location=str(migration_location),
    )

    migration_list = MigrationChain(migration_location, logger)
    migration_list.build_list()

    try:
        async with Connection(dsn=settings.clickhouse_url) as conn:
            await create_migratino_table(conn, logger)
    except Exception as e:
        logger.error(
            operation,
            error="Failed to create migration table",
            details=str(e),
            exc_info=True,
        )
        return None

    try:
        async with Connection(dsn=settings.clickhouse_url) as conn:
            current_version = await get_current_db_migration_version(
                conn, logger
            )
    except Exception as e:
        logger.error(
            operation,
            error="Failed to retrieve migration version",
            details=str(e),
            exc_info=True,
        )
        return None

    if current_version is None:
        logger.error(
            operation,
            error="Failed to get current version, aborting migration",
        )
        return None

    return MigrationContext(
        logger=logger,
        migration_location=migration_location,
        migration_list=migration_list,
        current_version=current_version,
    )


@app.command
def init(
    folder_name: Optional[str] = None,
    /,
):
    setup_logging()
    logger: LoggerType = structlog.get_logger()

    config_path = Path.cwd() / "hermes.toml"

    if config_path.exists():
        logger.error(
            "init",
            error="Configuration file already exists",
            path=str(config_path),
        )
        return

    default_config = MigrationConfig()
    if folder_name:
        default_config.migrations_location = folder_name
    config_dict = default_config.model_dump(by_alias=True)

    with open(config_path, "w") as f:
        toml.dump(config_dict, f)

    logger.info(
        "init",
        message="Initialized hermes project",
        folder=folder_name,
        config_path=str(config_path),
    )


@app.command
def new(
    *,
    message: Annotated[str, Parameter(name=("--message", "-m"))],
    config_path: Annotated[
        str, Parameter(name="--config-path")
    ] = "hermes.toml",
):
    config = load_config(config_path)
    setup_logging(
        filename=str(config.log_file_path) if config.log_to_file else None,
        level=config.get_log_level,
        log_to_file=config.log_to_file,
        log_to_stream=config.log_to_stream,
    )
    logger: LoggerType = structlog.get_logger()
    version = uuid.uuid4().hex
    migration_location = cast(Path, config.get_migration_dir)

    migration_list = MigrationChain(migration_location, logger)
    migration_list.build_list()

    target_dir = migration_location / f"{version}--{message.replace(' ', '_')}"
    target_dir.mkdir()

    migration_info = MigrationInfo(
        message=message,
        version=version,
        previous_version=None,
        next_version=None,
        creation_date=datetime.now().isoformat(),
    )
    last_version = migration_list.tail

    if last_version:
        migration_info.previous_version = last_version.info.version
        last_version.info.next_version = version

        last_migration_dir = None
        for d in migration_location.iterdir():
            if is_valid_migration_directory(
                d,
                logger,
            ) and compare_migration_folder_name_with_version(
                version=last_version.info.version,
                full_folder_name=d.name,
            ):
                last_migration_dir = d
                break

        if last_migration_dir:
            last_info_file = last_migration_dir / "info.toml"
            with open(last_info_file, "w") as f:
                toml.dump(last_version.info.model_dump(by_alias=True), f)

    with open(target_dir / "info.toml", "w") as f:
        toml.dump(migration_info.model_dump(by_alias=True), f)
    (target_dir / "upgrade.sql").touch()
    (target_dir / "downgrade.sql").touch()

    logger.info("new-migration", version=version, at=target_dir.name)


@app.command
async def upgrade(
    revision: str,
    /,
    *,
    config_path: Annotated[
        str, Parameter(name="--config-path")
    ] = "hermes.toml",
):
    context = await initialize_migration_context(config_path, "upgrade")
    if context is None:  # Error occurred during initialization
        return

    logger = context["logger"]
    migration_location = context["migration_location"]
    migration_list = context["migration_list"]
    current_version = context["current_version"]
    logger.info("upgrade", revision=revision)

    if revision == "head":
        if migration_list.tail.info.version == current_version:
            logger.info("run-migration", message="Already at head")
            return

        migrations_to_run = []
        if not current_version:
            migration = migration_list.head
        else:
            current_migration = migration_list.find_by_version(current_version)
            if not current_migration:
                raise ValueError(
                    f"Current version {current_version} not found in migration chain"
                )
            migration = current_migration.next

        while migration:
            migrations_to_run.append(migration)
            migration = migration.next

    else:
        target_migration = migration_list.find_by_version(revision)
        if not target_migration:
            raise ValueError(
                f"Target version {revision} not found in migration chain"
            )

        if revision == current_version:
            logger.info(
                "run-migration", message=f"Already at version {revision}"
            )
            return

        migrations_to_run = [target_migration]

    if migrations_to_run:
        logger.info(
            "upgrade",
            message="Starting migration execution",
            versions_count=len(migrations_to_run),
        )
    else:
        logger.info("upgrade", message="no migration to run")
        return

    try:
        async with Connection(dsn=settings.clickhouse_url) as conn:
            await run_migration(
                versions=migrations_to_run,
                versions_dir=migration_location,
                mode="upgrade",
                connection=conn,
                logger=logger,
            )
        logger.info(
            "upgrade", message="Migration execution completed successfully"
        )
    except Exception as e:
        logger.error(
            "upgrade",
            error="Migration execution failed",
            details=str(e),
            exc_info=True,
        )
        return


@app.command
async def downgrade(
    revision: str,
    /,
    *,
    config_path: Annotated[
        str, Parameter(name="--config-path")
    ] = "hermes.toml",
):
    context = await initialize_migration_context(config_path, "downgrade")
    if context is None:
        return

    logger = context["logger"]
    migration_location = context["migration_location"]
    migration_list = context["migration_list"]
    current_version = context["current_version"]
    logger.info("downgrade", revision=revision)

    if not current_version:
        logger.info(
            "downgrade", message="Already at base (no migrations applied)"
        )
        return

    current_migration = migration_list.find_by_version(current_version)
    if not current_migration:
        logger.error(
            "downgrade",
            error=f"Current version {current_version} not found in migration chain",
        )
        return

    migrations_to_run = []

    if revision == "base":
        migration = current_migration
        while migration:
            migrations_to_run.append(migration)
            migration = migration.previous
    else:
        target_migration = migration_list.find_by_version(revision)
        if not target_migration:
            logger.error(
                "downgrade",
                error=f"Target version {revision} not found in migration chain",
            )
            return

        if revision == current_version:
            logger.info("downgrade", message=f"Already at version {revision}")
            return

        migration = current_migration
        while migration and migration.info.version != revision:
            migrations_to_run.append(migration)
            migration = migration.previous

        if not migration:
            logger.error(
                "downgrade",
                error=f"Cannot reach target version {revision} from current version {current_version}",
            )
            return

    if migrations_to_run:
        logger.info(
            "downgrade",
            message="Starting migration downgrade",
            versions_count=len(migrations_to_run),
        )
    else:
        logger.info("downgrade", message="no migration to run")
        return

    try:
        async with Connection(dsn=settings.clickhouse_url) as conn:
            await run_migration(
                versions=migrations_to_run,
                versions_dir=migration_location,
                mode="downgrade",
                connection=conn,
                logger=logger,
            )
        logger.info(
            "downgrade", message="Migration downgrade completed successfully"
        )
    except Exception as e:
        logger.error(
            "downgrade",
            error="Migration downgrade failed",
            details=str(e),
            exc_info=True,
        )
        return


if __name__ == "__main__":
    app()
