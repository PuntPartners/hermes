# import asyncio
# import sys
# import tempfile
# import threading
# from pathlib import Path
# from typing import Any
#
# import docker
# import structlog
# from asynch import Connection
#
# from .utils import run_migration
#
# logger: structlog.stdlib.BoundLogger = structlog.get_logger()
#
#
# class TestCheckpoint(Checkpoint):
#     """Test-specific checkpoint that uses a temporary file."""
#
#     def __init__(self, temp_file: Path, **data: Any):
#         super().__init__(**data)
#         self._temp_file = temp_file
#
#     @classmethod
#     def create_temp(cls, temp_file: Path) -> "TestCheckpoint":
#         """Create a temporary checkpoint for testing."""
#         latest_index = 0
#         for d in versions_dir.iterdir():
#             if not d.is_dir() or "--" not in d.name:
#                 continue
#             try:
#                 version = int(d.name.split("--", 1)[0])
#                 latest_index = max(version, latest_index)
#             except ValueError:
#                 continue
#
#         checkpoint = cls(temp_file, latest_index=latest_index, migrated_index=0)
#         checkpoint.persist()
#         return checkpoint
#
#     def persist(self) -> None:
#         """Persist to the temporary file."""
#         self._temp_file.write_text(self.model_dump_json(indent=2))
#
#
# def _stream_container_logs(container: docker.models.containers.Container, debug: bool) -> None:
#     """Stream container logs to stdout/stderr in a separate thread."""
#     if not debug:
#         return
#
#     def stream_logs():
#         try:
#             for line in container.logs(stream=True, follow=True):
#                 sys.stdout.write(f"[CONTAINER] {line.decode().rstrip()}\n")
#                 sys.stdout.flush()
#         except Exception as e:
#             sys.stderr.write(f"[LOG-STREAM-ERROR] {e}\n")
#
#     thread = threading.Thread(target=stream_logs, daemon=True)
#     thread.start()
#
#
# async def run_test_migrations(clickhouse_version: str, debug: bool = False) -> None:
#     """
#     Run migration tests using ClickHouse test containers.
#
#     :param clickhouse_version: ClickHouse version to use
#     :param debug: Enable debug mode to show container logs
#     """
#     client = docker.from_env()
#
#     # Start ClickHouse container
#     container_name = (
#         f"clickhouse-test-{asyncio.current_task().get_name() if asyncio.current_task() else 'main'}"
#     )
#
#     logger.info("test-migration", action="starting-container", version=clickhouse_version)
#
#     try:
#         # Remove existing container if it exists
#         try:
#             existing = client.containers.get(container_name)
#             existing.remove(force=True)
#         except docker.errors.NotFound:
#             pass
#
#         # Start new container
#         container = client.containers.run(
#             f"clickhouse/clickhouse-server:{clickhouse_version}",
#             name=container_name,
#             ports={"8123/tcp": None, "9000/tcp": None},
#             detach=True,
#             remove=True,
#             environment={
#                 "CLICKHOUSE_DB": "testdb",
#                 "CLICKHOUSE_USER": "testuser",
#                 "CLICKHOUSE_PASSWORD": "testpass",
#             },
#         )
#
#         # Start log streaming if debug mode is enabled
#         _stream_container_logs(container, debug)
#
#         # Wait for container to be ready
#         await asyncio.sleep(10)
#
#         # Get the mapped port
#         container.reload()
#         port_info = container.attrs["NetworkSettings"]["Ports"]["9000/tcp"]
#         if not port_info:
#             raise RuntimeError("Failed to get container port mapping")
#
#         host_port = port_info[0]["HostPort"]
#         dsn = f"clickhouse://testuser:testpass@localhost:{host_port}/testdb"
#
#         logger.info("test-migration", action="container-ready", dsn=dsn)
#
#         # Create temporary checkpoint file
#         with tempfile.NamedTemporaryFile(mode="w", suffix=".ckpt", delete=False) as tmp_file:
#             temp_checkpoint_path = Path(tmp_file.name)
#
#         try:
#             # Test migration up to latest
#             checkpoint = TestCheckpoint.create_temp(temp_checkpoint_path)
#             logger.info(
#                 "test-migration", action="upgrading-to-latest", latest=checkpoint.latest_index
#             )
#
#             async with Connection(dsn=dsn) as conn:
#                 if checkpoint.latest_index > 0:
#                     version_range = range(1, checkpoint.latest_index + 1)
#                     await run_migration(*version_range, mode="upgrade", connection=conn)
#                     checkpoint.migrated_index = checkpoint.latest_index
#                     checkpoint.persist()
#
#             logger.info(
#                 "test-migration", action="upgrade-complete", migrated=checkpoint.migrated_index
#             )
#
#             # Test migration down to base
#             logger.info("test-migration", action="downgrading-to-base")
#
#             async with Connection(dsn=dsn) as conn:
#                 if checkpoint.migrated_index > 0:
#                     version_range = range(checkpoint.migrated_index, 0, -1)
#                     await run_migration(*version_range, mode="downgrade", connection=conn)
#                     checkpoint.migrated_index = 0
#                     checkpoint.persist()
#
#             logger.info(
#                 "test-migration", action="downgrade-complete", migrated=checkpoint.migrated_index
#             )
#             logger.info(
#                 "test-migration", status="success", message="All migrations tested successfully"
#             )
#
#         finally:
#             # Clean up temp file
#             temp_checkpoint_path.unlink(missing_ok=True)
#
#     finally:
#         # Stop and remove container
#         try:
#             container.stop()
#         except Exception as e:
#             logger.warning("test-migration", action="cleanup-failed", error=str(e))
