import uuid
from datetime import datetime
from pathlib import Path
from typing import Annotated, cast

from asynch import Connection
from cannonball_writer.server.settings import settings
from cyclopts import App, Parameter
from src.migration_chain import MigrationChain
from src.schema import MigrationInfo
from src.utils import (
    compare_migration_folder_name_with_version,
    create_migratino_table,
    get_current_db_migration_version,
    is_valid_migration_directory,
    load_config,
    logger,
    run_migration,
)

from src.logging import setup_logging

app = App(name="ch-migrate")

setup_logging("hermes.log")


@app.command
def new(
    *,
    message: Annotated[str, Parameter(name=("--message", "-m"))],
    config_path: Annotated[str, Parameter(name="--config-path")] = "migration.toml",
):
    version = uuid.uuid4().hex
    config = load_config(config_path)
    migration_location = cast(Path, config.get_migration_dir)

    migration_list = MigrationChain(migration_location)
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
                d
            ) and compare_migration_folder_name_with_version(
                version=last_version.info.version,
                full_folder_name=d.name,
            ):
                last_migration_dir = d
                break

        if last_migration_dir:
            last_info_file = last_migration_dir / "info.json"
            last_info_file.write_text(last_version.info.model_dump_json(indent=4))

    (target_dir / "info.json").write_text(migration_info.model_dump_json(indent=4))
    (target_dir / "upgrade.sql").touch()
    (target_dir / "downgrade.sql").touch()

    logger.info("new-migration", version=version, at=target_dir.name)


@app.command
async def upgrade(
    revision: str,
    /,
    *,
    config_path: Annotated[str, Parameter(name="--config-path")] = "migration.toml",
):
    try:
        config = load_config(config_path)
    except FileNotFoundError as e:
        logger.error(
            "upgrade",
            error="Config file not found",
            path=config_path,
            details=str(e),
            exc_info=True,
        )
        return

    migration_location = cast(Path, config.get_migration_dir)
    logger.info(
        "upgrade", revision=revision, migration_location=str(migration_location)
    )

    migration_list = MigrationChain(migration_location)
    migration_list.build_list()

    try:
        async with Connection(dsn=settings.CLICKHOUSE_DSN) as conn:
            await create_migratino_table(conn)
    except Exception as e:
        logger.error(
            "upgrade",
            error="Failed to create migration table",
            details=str(e),
            exc_info=True,
        )
        return

    try:
        async with Connection(dsn=settings.CLICKHOUSE_DSN) as conn:
            current_version = await get_current_db_migration_version(conn)
    except Exception as e:
        logger.error(
            "upgrade",
            error="Failed to retrieve migration version",
            details=str(e),
            exc_info=True,
        )
        return

    if current_version is None:
        logger.error(
            "upgrade", error="Failed to get current version, aborting migration"
        )
        return

    if not migration_list.head:
        return

    if not migration_list.tail:
        return

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
            raise ValueError(f"Target version {revision} not found in migration chain")

        if revision == current_version:
            logger.info("run-migration", message=f"Already at version {revision}")
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
        async with Connection(dsn=settings.CLICKHOUSE_DSN) as conn:
            await run_migration(
                versions=migrations_to_run,
                versions_dir=migration_location,
                mode="upgrade",
                connection=conn,
            )
        logger.info("upgrade", message="Migration execution completed successfully")
    except Exception as e:
        logger.error(
            "upgrade", error="Migration execution failed", details=str(e), exc_info=True
        )
        return


@app.command
async def test(
    *,
    clickhouse_version: Annotated[str, Parameter(name="--clickhouse-version")] = "25.5",
    debug: Annotated[bool, Parameter(name="--debug")] = False,
    config_path: Annotated[str, Parameter(name="--config-path")] = "migration.toml",
):
    """
    Test migrations using ClickHouse test containers.

    :param clickhouse_version: ClickHouse version to use (default: 25.5)
    :param debug: Enable debug mode to show container logs
    """
    # await run_test_migrations(clickhouse_version, debug=debug)
    return


if __name__ == "__main__":
    app()
