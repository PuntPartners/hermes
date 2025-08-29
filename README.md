```
Usage: ch-migrate COMMAND

╭─ Commands ──────────────────────────────────────────────────────────╮
│ new        Create a new migration.                                  │
│ run        Run migrations.                                          │
│ test       Test migrations using ClickHouse test containers.        │
│ --help -h  Display this message and exit.                           │
│ --version  Display application version.                             │
╰─────────────────────────────────────────────────────────────────────╯
```

Migrations are managed using the `migration` package. To create a new migration, run:

```bash
uv run ch-migrate new -m "<your message>"
```

For upgrades, run:

```bash
uv run ch-migrate run <head | positive int>
```

For downgrades, run:

```bash
uv run ch-migrate run <base | negative int>
```

## Testing Migrations

To test migrations using ClickHouse test containers, run:

```bash
uv run ch-migrate test
```

Options:

- `--clickhouse-version`: Specify ClickHouse version (default: 25.5)
- `--debug`: Enable debug mode to show container logs

Examples:

```bash
# Test with default ClickHouse version
uv run ch-migrate test

# Test with specific ClickHouse version
uv run ch-migrate test --clickhouse-version 24.8

# Test with debug logging enabled
uv run ch-migrate test --debug

# Combined options
uv run ch-migrate test --clickhouse-version 25.5 --debug
```

The test command automatically:

1. Spins up a ClickHouse Docker container
2. Runs all migrations from base to latest version
3. Downgrades from latest back to base version
4. Verifies both upgrade and downgrade paths work correctly
