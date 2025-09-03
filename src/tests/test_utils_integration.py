from pathlib import Path

import pytest
import toml
from asynch import Connection

from src.env import Settings
from src.logger import LoggerType
from src.migration_chain import MigrationChain
from src.schema import MigrationConfig, MigrationInfo
from src.utils import (
    create_migratino_table,
    execute_sql_statements,
    get_current_db_migration_version,
    is_valid_migration_directory,
    load_config,
    run_migration,
    update_migration_version,
)


class TestDatabaseOperations:
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_create_migration_table(
        self, test_settings: Settings, test_logger: LoggerType
    ):
        async with Connection(dsn=test_settings.clickhouse_url) as conn:
            # Should create table successfully
            await create_migratino_table(conn, test_logger)

            # Verify table exists by querying it
            async with conn.cursor() as cursor:
                await cursor.execute("SHOW TABLES LIKE 'ch_migrations'")
                result = await cursor.fetchone()
                assert result is not None
                assert result[0] == "ch_migrations"

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_get_current_db_migration_version_empty(
        self, test_settings: Settings, test_logger: LoggerType
    ):
        async with Connection(dsn=test_settings.clickhouse_url) as conn:
            await create_migratino_table(conn, test_logger)
            version = await get_current_db_migration_version(conn, test_logger)
            assert version == ""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_update_and_get_migration_version(
        self, test_settings: Settings, test_logger: LoggerType
    ):
        """Test updating and retrieving migration version."""
        async with Connection(dsn=test_settings.clickhouse_url) as conn:
            await create_migratino_table(conn, test_logger)

            # Update to a specific version
            test_version = "test_version_123"
            success = await update_migration_version(
                conn, test_version, test_logger
            )
            assert success is True

            # Retrieve the version
            current_version = await get_current_db_migration_version(
                conn, test_logger
            )
            assert current_version == test_version

            # Update to a different version
            new_version = "test_version_456"
            success = await update_migration_version(
                conn, new_version, test_logger
            )
            assert success is True

            # Verify it was updated
            current_version = await get_current_db_migration_version(
                conn, test_logger
            )
            assert current_version == new_version

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_execute_sql_statements_success(
        self, test_settings: Settings, test_logger: LoggerType
    ):
        """Test executing SQL statements successfully."""
        async with Connection(dsn=test_settings.clickhouse_url) as conn:
            statements = [
                "CREATE TABLE test_table (id UInt32) ENGINE = Memory",
                "INSERT INTO test_table VALUES (1)",
                "INSERT INTO test_table VALUES (2)",
            ]

            success = await execute_sql_statements(
                conn, statements, "test_version", test_logger
            )
            assert success is True

            # Verify the table and data exist
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM test_table")
                result = await cursor.fetchone()
                print(result)
                assert result[0] == 2

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_execute_sql_statements_failure(
        self, test_settings: Settings, test_logger: LoggerType
    ):
        """Test executing invalid SQL statements."""
        async with Connection(dsn=test_settings.clickhouse_url) as conn:
            statements = [
                "CREATE TABLE test_table2 (id UInt32) ENGINE = Memory",
                "INVALID SQL STATEMENT",  # This should cause failure
                "INSERT INTO test_table2 VALUES (1)",
            ]

            success = await execute_sql_statements(
                conn, statements, "test_version", test_logger
            )
            assert success is False


class TestConfigOperations:
    """Test configuration-related utility functions."""

    @pytest.mark.integration
    def test_load_config_default(self, tmp_path: Path, test_logger: LoggerType):
        """Test loading default configuration."""
        config_file = tmp_path / "test_config.toml"
        config_file.write_text("""
[migrations]
location = "test_versions"
""")

        config = load_config(str(config_file))
        assert isinstance(config, MigrationConfig)
        assert config.log_level == "info"  # default value
        assert config.log_to_file is True  # default value
        assert config.log_to_stream is True  # default value

    def test_load_config_custom(self, tmp_path: Path, test_logger: LoggerType):
        """Test loading custom configuration."""
        config_file = tmp_path / "test_config.toml"
        config_file.write_text("""
migrations-location = "custom_versions"
log-level = "debug"
log-to-file = false
log-to-stream = true
log-file-path = "custom.log"
""")

        config = load_config(str(config_file))
        assert config.migrations_location == "custom_versions"
        assert config.log_level == "debug"
        assert config.log_to_file is False
        assert config.log_to_stream is True
        assert str(config.log_file_path) == "custom.log"

    def test_load_config_file_not_found(self, test_logger: LoggerType):
        """Test loading non-existent configuration file."""
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent.toml")


class TestMigrationDirectoryValidation:
    """Test migration directory validation functions."""

    @pytest.mark.integration
    def test_is_valid_migration_directory_valid(
        self, tmp_path: Path, test_logger: LoggerType
    ):
        migration_dir = tmp_path / "version123--create_table"
        migration_dir.mkdir()

        with open(migration_dir / "info.toml", "w") as f:
            toml.dump({"version": "123"}, f)
        (migration_dir / "upgrade.sql").write_text("CREATE TABLE test();")
        (migration_dir / "downgrade.sql").write_text("DROP TABLE test;")

        assert is_valid_migration_directory(migration_dir, test_logger) is True

    @pytest.mark.integration
    def test_is_valid_migration_directory_missing_separator(
        self, tmp_path: Path, test_logger: LoggerType
    ):
        """Test validation fails when directory name lacks '--' separator."""
        migration_dir = tmp_path / "version123_create_table"  # No '--'
        migration_dir.mkdir()

        assert is_valid_migration_directory(migration_dir, test_logger) is False

    @pytest.mark.integration
    def test_is_valid_migration_directory_missing_info_toml(
        self, tmp_path: Path, test_logger: LoggerType
    ):
        """Test validation fails when info.toml is missing."""
        migration_dir = tmp_path / "version123--create_table"
        migration_dir.mkdir()

        # Create SQL files but not info.toml
        (migration_dir / "upgrade.sql").write_text("CREATE TABLE test();")
        (migration_dir / "downgrade.sql").write_text("DROP TABLE test;")

        assert is_valid_migration_directory(migration_dir, test_logger) is False

    @pytest.mark.integration
    def test_is_valid_migration_directory_missing_sql_files(
        self, tmp_path: Path, test_logger: LoggerType
    ):
        """Test validation fails when SQL files are missing."""
        migration_dir = tmp_path / "version123--create_table"
        migration_dir.mkdir()

        # Create info.toml but not SQL files
        with open(migration_dir / "info.toml", "w") as f:
            toml.dump({"version": "123"}, f)

        assert is_valid_migration_directory(migration_dir, test_logger) is False

    @pytest.mark.integration
    def test_is_valid_migration_directory_not_directory(
        self, tmp_path: Path, test_logger: LoggerType
    ):
        """Test validation fails when path is not a directory."""
        file_path = tmp_path / "not_a_directory.txt"
        file_path.write_text("This is a file")

        assert is_valid_migration_directory(file_path, test_logger) is False


class TestMigrationExecution:
    """Test migration execution functions."""

    def _create_test_migration(
        self, migrations_dir: Path, version: str, message: str
    ) -> Path:
        """Helper to create a test migration directory."""
        migration_dir = migrations_dir / f"{version}--{message}"
        migration_dir.mkdir()

        info = MigrationInfo(
            message=message,
            version=version,
            previous_version=None,
            next_version=None,
            creation_date="2024-01-01T00:00:00",
        )

        with open(migration_dir / "info.toml", "w") as f:
            toml.dump(info.model_dump(by_alias=True), f)
        (migration_dir / "upgrade.sql").write_text(
            f"CREATE TABLE {message} (id UInt32) ENGINE = Memory;"
        )
        (migration_dir / "downgrade.sql").write_text(
            f"DROP TABLE IF EXISTS {message};"
        )

        return migration_dir

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_run_migration_upgrade(
        self, tmp_path: Path, test_settings: Settings, test_logger: LoggerType
    ):
        """Test running an upgrade migration."""
        # Create migration directory structure
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()

        # Create test migration
        self._create_test_migration(migrations_dir, "v001", "create_users")

        # Create migration chain
        migration_chain = MigrationChain(migrations_dir, test_logger)
        migration_chain.build_list()

        assert migration_chain.head is not None

        async with Connection(dsn=test_settings.clickhouse_url) as conn:
            await create_migratino_table(conn, test_logger)

            # Run upgrade migration
            await run_migration(
                versions=[migration_chain.head],
                versions_dir=migrations_dir,
                mode="upgrade",
                connection=conn,
                logger=test_logger,
            )

            # Verify table was created
            async with conn.cursor() as cursor:
                await cursor.execute("SHOW TABLES LIKE 'create_users'")
                result = await cursor.fetchone()
                assert result is not None

            # Verify migration version was updated
            current_version = await get_current_db_migration_version(
                conn, test_logger
            )
            assert current_version == "v001"

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_run_migration_downgrade(
        self, tmp_path: Path, test_settings: Settings, test_logger: LoggerType
    ):
        """Test running a downgrade migration."""
        # Create migration directory structure
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()

        # Create test migration
        self._create_test_migration(migrations_dir, "v001", "create_orders")

        # Create migration chain
        migration_chain = MigrationChain(migrations_dir, test_logger)
        migration_chain.build_list()

        assert migration_chain.head is not None

        async with Connection(dsn=test_settings.clickhouse_url) as conn:
            await create_migratino_table(conn, test_logger)

            await run_migration(
                versions=[migration_chain.head],
                versions_dir=migrations_dir,
                mode="upgrade",
                connection=conn,
                logger=test_logger,
            )

            async with conn.cursor() as cursor:
                await cursor.execute("SHOW TABLES LIKE 'create_orders'")
                result = await cursor.fetchone()
                assert result is not None

            await run_migration(
                versions=[migration_chain.head],
                versions_dir=migrations_dir,
                mode="downgrade",
                connection=conn,
                logger=test_logger,
            )

            async with conn.cursor() as cursor:
                await cursor.execute("SHOW TABLES LIKE 'create_orders'")
                result = await cursor.fetchone()
                assert result is None

            current_version = await get_current_db_migration_version(
                conn, test_logger
            )
            assert current_version == ""
