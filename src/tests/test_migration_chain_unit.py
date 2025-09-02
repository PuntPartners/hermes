from pathlib import Path

import pytest
import structlog
import toml
from toml import TomlDecodeError

from src.logger import LoggerType
from src.migration_chain import MigrationChain, MigrationNode
from src.schema import MigrationInfo


class TestMigrationNode:
    """Test MigrationNode Pydantic model."""

    def test_migration_node_creation(self):
        """Test creating a MigrationNode with migration info."""
        migration_info = MigrationInfo(
            message="create users table",
            version="v001",
            previous_version=None,
            next_version="v002",
            creation_date="2024-01-01T00:00:00",
        )

        node = MigrationNode(info=migration_info)

        assert node.info.message == "create users table"
        assert node.info.version == "v001"
        assert node.info.next_version == "v002"
        assert node.previous is None
        assert node.next is None

    def test_migration_node_with_links(self):
        """Test creating MigrationNode with previous/next links."""
        info1 = MigrationInfo(
            message="create users",
            version="v001",
            previous_version=None,
            next_version="v002",
            creation_date="2024-01-01T00:00:00",
        )

        info2 = MigrationInfo(
            message="create posts",
            version="v002",
            previous_version="v001",
            next_version=None,
            creation_date="2024-01-02T00:00:00",
        )

        node1 = MigrationNode(info=info1)
        node2 = MigrationNode(info=info2, previous=node1)
        node1.next = node2

        assert node1.next == node2
        assert node2.previous == node1
        assert node1.info.version == "v001"
        assert node2.info.version == "v002"

    def test_migration_node_model_validation(self):
        """Test that MigrationNode validates required fields."""
        # Should work with valid MigrationInfo
        valid_info = MigrationInfo(
            message="test",
            version="v001",
            previous_version=None,
            next_version=None,
            creation_date="2024-01-01T00:00:00",
        )
        node = MigrationNode(info=valid_info)
        assert node.info.message == "test"


class TestMigrationChain:
    """Test MigrationChain class."""

    @pytest.fixture
    def mock_logger(self) -> LoggerType:
        """Create a mock logger for testing."""
        return structlog.get_logger("test")

    @pytest.fixture
    def empty_migrations_dir(self, tmp_path: Path) -> Path:
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()
        return migrations_dir

    def test_migration_chain_initialization(
        self, empty_migrations_dir: Path, mock_logger: LoggerType
    ):
        """Test MigrationChain initialization."""
        chain = MigrationChain(empty_migrations_dir, mock_logger)

        assert chain.head is None
        assert chain.tail is None
        assert chain._is_initialized is False
        assert chain._versions_dir == empty_migrations_dir
        assert chain._logger == mock_logger

    def test_build_list_empty_directory(
        self, empty_migrations_dir: Path, mock_logger: LoggerType
    ):
        """Test building chain from empty directory."""
        chain = MigrationChain(empty_migrations_dir, mock_logger)
        chain.build_list()

        assert chain.head is None
        assert chain.tail is None
        assert chain._is_initialized is False

    def test_build_list_single_migration(
        self, tmp_path: Path, mock_logger: LoggerType
    ):
        """Test building chain with single migration."""
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()

        # Create a single migration
        migration_dir = migrations_dir / "v001--create_users"
        migration_dir.mkdir()

        migration_info = MigrationInfo(
            message="create_users",
            version="v001",
            previous_version=None,
            next_version=None,
            creation_date="2024-01-01T00:00:00",
        )

        with open(migration_dir / "info.toml", "w") as f:
            toml.dump(migration_info.model_dump(by_alias=True), f)
        (migration_dir / "upgrade.sql").write_text("CREATE TABLE users();")
        (migration_dir / "downgrade.sql").write_text("DROP TABLE users;")

        chain = MigrationChain(migrations_dir, mock_logger)
        chain.build_list()

        assert chain.head is not None
        assert chain.tail is not None
        assert chain.head == chain.tail
        assert chain.head.info.version == "v001"
        assert chain.head.info.message == "create_users"

    def test_build_list_multiple_migrations_linked(
        self, tmp_path: Path, mock_logger: LoggerType
    ):
        """Test building chain with multiple linked migrations."""
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()

        # Create migration chain: v001 -> v002 -> v003
        migrations = [
            ("v001", "create_users", None, "v002"),
            ("v002", "create_posts", "v001", "v003"),
            ("v003", "add_indexes", "v002", None),
        ]

        for version, message, prev_version, next_version in migrations:
            migration_dir = migrations_dir / f"{version}--{message}"
            migration_dir.mkdir()

            migration_info = MigrationInfo(
                message=message,
                version=version,
                previous_version=prev_version,
                next_version=next_version,
                creation_date="2024-01-01T00:00:00",
            )

            with open(migration_dir / "info.toml", "w") as f:
                toml.dump(migration_info.model_dump(by_alias=True), f)
            (migration_dir / "upgrade.sql").write_text(f"-- {message}")
            (migration_dir / "downgrade.sql").write_text(
                f"-- rollback {message}"
            )

        chain = MigrationChain(migrations_dir, mock_logger)
        chain.build_list()

        # Check chain structure
        assert chain.head is not None
        assert chain.tail is not None
        assert chain.head.info.version == "v001"
        assert chain.tail.info.version == "v003"

        # Check links
        assert chain.head.next.info.version == "v002"
        assert chain.head.next.next.info.version == "v003"
        assert chain.tail.previous.info.version == "v002"
        assert chain.tail.previous.previous.info.version == "v001"

    def test_build_list_broken_chain(
        self, tmp_path: Path, mock_logger: LoggerType
    ):
        """Test building chain with broken links."""
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()

        # Create migration with missing next migration
        migration_dir = migrations_dir / "v001--create_users"
        migration_dir.mkdir()

        migration_info = MigrationInfo(
            message="create_users",
            version="v001",
            previous_version=None,
            next_version="v002",  # This migration doesn't exist
            creation_date="2024-01-01T00:00:00",
        )

        with open(migration_dir / "info.toml", "w") as f:
            toml.dump(migration_info.model_dump(by_alias=True), f)
        (migration_dir / "upgrade.sql").write_text("CREATE TABLE users();")
        (migration_dir / "downgrade.sql").write_text("DROP TABLE users;")

        chain = MigrationChain(migrations_dir, mock_logger)
        chain.build_list()

        # Should still create head but not find next
        assert chain.head is not None
        assert chain.tail is not None
        assert chain.head == chain.tail
        assert chain.head.info.version == "v001"

    def test_find_by_version_existing(
        self, tmp_path: Path, mock_logger: LoggerType
    ):
        """Test finding existing migration by version."""
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()

        # Create two migrations
        for i, (version, message) in enumerate(
            [("v001", "create_users"), ("v002", "create_posts")], 1
        ):
            migration_dir = migrations_dir / f"{version}--{message}"
            migration_dir.mkdir()

            prev_version = "v001" if i == 2 else None
            next_version = "v002" if i == 1 else None

            migration_info = MigrationInfo(
                message=message,
                version=version,
                previous_version=prev_version,
                next_version=next_version,
                creation_date="2024-01-01T00:00:00",
            )

            with open(migration_dir / "info.toml", "w") as f:
                toml.dump(migration_info.model_dump(by_alias=True), f)
            (migration_dir / "upgrade.sql").write_text(f"-- {message}")
            (migration_dir / "downgrade.sql").write_text(
                f"-- rollback {message}"
            )

        chain = MigrationChain(migrations_dir, mock_logger)

        # Test finding existing migrations
        found_v001 = chain.find_by_version("v001")
        found_v002 = chain.find_by_version("v002")

        assert found_v001 is not None
        assert found_v001.info.version == "v001"
        assert found_v001.info.message == "create_users"

        assert found_v002 is not None
        assert found_v002.info.version == "v002"
        assert found_v002.info.message == "create_posts"

    def test_find_by_version_nonexistent(
        self, empty_migrations_dir: Path, mock_logger: LoggerType
    ):
        """Test finding non-existent migration by version."""
        chain = MigrationChain(empty_migrations_dir, mock_logger)

        found = chain.find_by_version("nonexistent")
        assert found is None

    def test_invalid_migration_directory_ignored(
        self, tmp_path: Path, mock_logger: LoggerType
    ):
        """Test that invalid migration directories are ignored."""
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()

        # Create invalid migration directory (no info.toml)
        invalid_dir = migrations_dir / "v001--invalid"
        invalid_dir.mkdir()
        (invalid_dir / "upgrade.sql").write_text("CREATE TABLE test();")
        # Missing info.toml and downgrade.sql

        # Create valid migration directory
        valid_dir = migrations_dir / "v002--valid"
        valid_dir.mkdir()

        migration_info = MigrationInfo(
            message="valid",
            version="v002",
            previous_version=None,
            next_version=None,
            creation_date="2024-01-01T00:00:00",
        )

        with open(valid_dir / "info.toml", "w") as f:
            toml.dump(migration_info.model_dump(by_alias=True), f)
        (valid_dir / "upgrade.sql").write_text("CREATE TABLE valid();")
        (valid_dir / "downgrade.sql").write_text("DROP TABLE valid;")

        chain = MigrationChain(migrations_dir, mock_logger)
        chain.build_list()

        # Only valid migration should be in chain
        assert chain.head is not None
        assert chain.head.info.version == "v002"
        assert chain.head.info.message == "valid"

    def test_build_list_idempotent(
        self, tmp_path: Path, mock_logger: LoggerType
    ):
        """Test that build_list can be called multiple times safely."""
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()

        # Create migration
        migration_dir = migrations_dir / "v001--create_users"
        migration_dir.mkdir()

        migration_info = MigrationInfo(
            message="create_users",
            version="v001",
            previous_version=None,
            next_version=None,
            creation_date="2024-01-01T00:00:00",
        )

        with open(migration_dir / "info.toml", "w") as f:
            toml.dump(migration_info.model_dump(by_alias=True), f)
        (migration_dir / "upgrade.sql").write_text("CREATE TABLE users();")
        (migration_dir / "downgrade.sql").write_text("DROP TABLE users;")

        chain = MigrationChain(migrations_dir, mock_logger)

        # Call build_list multiple times
        chain.build_list()
        first_head = chain.head

        chain.build_list()
        second_head = chain.head

        chain.build_list()
        third_head = chain.head

        # Should be the same object each time
        assert first_head == second_head == third_head
        assert chain._is_initialized is True

    def test_malformed_info_toml_ignored(
        self, tmp_path: Path, mock_logger: LoggerType
    ):
        """Test that directories with malformed info.toml are ignored."""
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()

        # Create migration with malformed TOML
        bad_migration_dir = migrations_dir / "v001--bad_json"
        bad_migration_dir.mkdir()
        (bad_migration_dir / "info.toml").write_text("invalid toml content [")
        (bad_migration_dir / "upgrade.sql").write_text("CREATE TABLE test();")
        (bad_migration_dir / "downgrade.sql").write_text("DROP TABLE test;")

        # Create valid migration
        good_migration_dir = migrations_dir / "v002--good"
        good_migration_dir.mkdir()

        migration_info = MigrationInfo(
            message="good",
            version="v002",
            previous_version=None,
            next_version=None,
            creation_date="2024-01-01T00:00:00",
        )

        with open(good_migration_dir / "info.toml", "w") as f:
            toml.dump(migration_info.model_dump(by_alias=True), f)
        (good_migration_dir / "upgrade.sql").write_text("CREATE TABLE good();")
        (good_migration_dir / "downgrade.sql").write_text("DROP TABLE good;")

        chain = MigrationChain(migrations_dir, mock_logger)

        # Should handle the malformed TOML gracefully and only include valid migration
        with pytest.raises(TomlDecodeError):
            chain.build_list()
