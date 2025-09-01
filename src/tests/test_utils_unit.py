from pathlib import Path

import pytest

from src.logger import LoggerType
from src.schema import MigrationConfig
from src.utils import (
    compare_migration_folder_name_with_version,
    is_valid_migration_directory,
    load_config,
)


class TestComparisonFunctions:
    def test_compare_migration_folder_name_with_version_match(self):
        version = "abc123"
        folder_name = "abc123--create_users_table"

        result = compare_migration_folder_name_with_version(
            version=version, full_folder_name=folder_name
        )
        assert result is True

    def test_compare_migration_folder_name_with_version_no_match(self):
        version = "abc123"
        folder_name = "def456--create_users_table"

        result = compare_migration_folder_name_with_version(
            version=version, full_folder_name=folder_name
        )
        assert result is False

    def test_compare_migration_folder_name_with_version_no_separator(self):
        version = "abc123"
        folder_name = "abc123_create_users_table"  # No '--' separator

        result = compare_migration_folder_name_with_version(
            version=version, full_folder_name=folder_name
        )
        assert result is False


class TestMigrationDirectoryValidation:
    """Test migration directory validation without database."""

    @pytest.fixture
    def mock_logger(self) -> LoggerType:
        """Create a mock logger for testing."""
        import structlog

        return structlog.get_logger("test")

    def test_is_valid_migration_directory_complete(
        self, tmp_path: Path, mock_logger: LoggerType
    ):
        """Test validation of a complete migration directory."""
        migration_dir = tmp_path / "abc123--create_table"
        migration_dir.mkdir()

        # Create all required files
        (migration_dir / "info.json").write_text(
            '{"version": "abc123", "message": "create table"}'
        )
        (migration_dir / "upgrade.sql").write_text("CREATE TABLE test();")
        (migration_dir / "downgrade.sql").write_text("DROP TABLE test;")

        result = is_valid_migration_directory(migration_dir, mock_logger)
        assert result is True

    def test_is_valid_migration_directory_missing_info(
        self, tmp_path: Path, mock_logger: LoggerType
    ):
        """Test validation fails when info.json is missing."""
        migration_dir = tmp_path / "abc123--create_table"
        migration_dir.mkdir()

        (migration_dir / "upgrade.sql").write_text("CREATE TABLE test();")
        (migration_dir / "downgrade.sql").write_text("DROP TABLE test;")
        # info.json missing

        result = is_valid_migration_directory(migration_dir, mock_logger)
        assert result is False

    def test_is_valid_migration_directory_missing_upgrade_sql(
        self, tmp_path: Path, mock_logger: LoggerType
    ):
        """Test validation fails when upgrade.sql is missing."""
        migration_dir = tmp_path / "abc123--create_table"
        migration_dir.mkdir()

        (migration_dir / "info.json").write_text('{"version": "abc123"}')
        (migration_dir / "downgrade.sql").write_text("DROP TABLE test;")
        # upgrade.sql missing

        result = is_valid_migration_directory(migration_dir, mock_logger)
        assert result is False

    def test_is_valid_migration_directory_missing_downgrade_sql(
        self, tmp_path: Path, mock_logger: LoggerType
    ):
        """Test validation fails when downgrade.sql is missing."""
        migration_dir = tmp_path / "abc123--create_table"
        migration_dir.mkdir()

        (migration_dir / "info.json").write_text('{"version": "abc123"}')
        (migration_dir / "upgrade.sql").write_text("CREATE TABLE test();")
        # downgrade.sql missing

        result = is_valid_migration_directory(migration_dir, mock_logger)
        assert result is False

    def test_is_valid_migration_directory_bad_separator(
        self, tmp_path: Path, mock_logger: LoggerType
    ):
        """Test validation fails when directory name has wrong separator."""
        migration_dir = (
            tmp_path / "abc123_create_table"
        )  # Using '_' instead of '--'
        migration_dir.mkdir()

        (migration_dir / "info.json").write_text('{"version": "abc123"}')
        (migration_dir / "upgrade.sql").write_text("CREATE TABLE test();")
        (migration_dir / "downgrade.sql").write_text("DROP TABLE test;")

        result = is_valid_migration_directory(migration_dir, mock_logger)
        assert result is False


class TestConfigLoading:
    """Test configuration loading functions."""

    @pytest.fixture
    def mock_logger(self) -> LoggerType:
        """Create a mock logger for testing."""
        import structlog

        return structlog.get_logger("test")

    def test_load_config_minimal(self, tmp_path: Path, mock_logger: LoggerType):
        """Test loading minimal valid configuration."""
        config_file = tmp_path / "minimal.toml"
        config_file.write_text("""
# Minimal config - should use defaults
""")

        config = load_config(str(config_file))

        assert isinstance(config, MigrationConfig)
        assert config.migrations_location == "versions"  # default
        assert config.log_level == "info"  # default
        assert config.log_to_file is True  # default
        assert config.log_to_stream is True  # default

    def test_load_config_full(self, tmp_path: Path):
        """Test loading complete configuration."""
        config_file = tmp_path / "full.toml"
        config_file.write_text("""
migrations-location = "custom_migrations"
log-level = "debug"
log-to-file = false
log-to-stream = true
log-file-path = "custom.log"
""")

        config = load_config(str(config_file))

        assert config.migrations_location == "custom_migrations"
        assert config.log_level == "debug"
        assert config.log_to_file is False
        assert config.log_to_stream is True
        assert str(config.log_file_path) == "custom.log"

    def test_load_config_invalid_log_level(
        self,
        tmp_path: Path,
    ):
        """Test loading config with invalid log level."""
        config_file = tmp_path / "invalid.toml"
        config_file.write_text("""
log-level = "invalid_level"
""")

        with pytest.raises(ValueError):
            load_config(str(config_file))

    def test_load_config_nonexistent_file(self):
        """Test loading non-existent configuration file."""
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent.toml")
