# Hermes

**Maintain your ClickHouse migrations.**

Hermes is a simple database migration tool specifically designed for ClickHouse databases.

## Installation

### Prerequisites

- Python 3.12 or higher
- ClickHouse server
- Docker (for running tests)

### Install from source

```bash
git clone https://github.com/your-org/hermes.git
cd hermes
uv sync
```

## Quick Start

1. **Create a configuration file** (`hermes.toml`):

```toml
migrations-location = "migrations"
log-level = "info"
log-to-file = true
log-to-stream = true
log-file-path = "hermes.log"
```

2. **Set up environment variables** (`.env`):

```bash
# Option 1: Direct URI
CLICKHOUSE_URI=clickhouse://user:password@localhost:9002/default

# Option 2: Individual parameters
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9002
CLICKHOUSE_DATABASE=default
CLICKHOUSE_USER=test
CLICKHOUSE_PASSWORD=test
```

3. **Create your first migration**:

```bash
uv run hermes new --message "create users table"
```

4. **Run migrations**:

```bash
uv run hermes upgrade head
```

## Usage

### Commands

#### `new` - Create a new migration

Create a new migration file with upgrade and downgrade SQL scripts.

```bash
uv run hermes new --message "your migration description"
uv run hermes new -m "create users table"

# With custom config
uv run hermes new -m "add indexes" --config-path custom.toml
```

**Options:**

- `--message, -m` (required): Description of the migration
- `--config-path`: Path to configuration file (default: `hermes.toml`)

#### `upgrade` - Apply migrations

Run migrations forward to upgrade your database schema.

```bash
# Upgrade to latest (head)
uv run hermes upgrade head

# Upgrade to specific version
uv run hermes upgrade abc123def456

# With custom config
uv run hermes upgrade head --config-path custom.toml
```

**Options:**

- `revision` (required): Target revision (`head` for latest, or specific version ID)
- `--config-path`: Path to configuration file (default: `hermes.toml`)

#### `downgrade` - Rollback migrations

Run migrations backward to downgrade your database schema.

```bash
# Downgrade to base (remove all migrations)
uv run hermes downgrade base

# Downgrade to specific version
uv run hermes downgrade abc123def456

# With custom config
uv run hermes downgrade base --config-path custom.toml
```

**Options:**

- `revision` (required): Target revision (`base` for empty database, or specific version ID)
- `--config-path`: Path to configuration file (default: `hermes.toml`)

### Configuration

Hermes uses TOML configuration files. Create a `hermes.toml` file in your project root:

```toml
# Migration settings
migrations-location = "migrations"    # Directory to store migration files

# Logging configuration
log-level = "info"                   # debug, info, warning, error, critical
log-to-file = true                   # Write logs to file
log-to-stream = true                 # Write logs to console
log-file-path = "hermes.log"         # Log file path
```

### Environment Variables

Set up your ClickHouse connection using environment variables:

**Option 1: Direct URI**

```bash
CLICKHOUSE_URI=clickhouse://user:password@host:port/database
```

**Option 2: Individual Parameters**

```bash
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9002
CLICKHOUSE_DATABASE=default
CLICKHOUSE_USER=test
CLICKHOUSE_PASSWORD=test
```

### Migration Files

When you create a new migration, Hermes generates a directory structure:

```
migrations/
 abc123def456_create_users_table/
     info.toml        # Migration metadata
     upgrade.sql      # Forward migration SQL
     downgrade.sql    # Rollback migration SQL
```

**Example upgrade.sql:**

```sql
CREATE TABLE users (
    id UInt64,
    name String,
    email String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;
```

**Example downgrade.sql:**

```sql
DROP TABLE IF EXISTS users;
```

## Development

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/your-org/hermes.git
cd hermes

# Install dependencies
uv sync --group dev

# Install pre-commit hooks
uv run pre-commit install
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run only unit tests (fast)
uv run pytest -m "not integration"

# Run only integration tests (requires Docker)
uv run pytest -m integration

# Run specific test file
uv run pytest src/tests/test_utils_unit.py -v
```

#### For Podman Users

To run tests with Podman instead of Docker:

```bash
# Enable Podman socket
systemctl --user enable --now podman.socket

# Set environment variables and run tests
export DOCKER_HOST="unix:///run/user/$UID/podman/podman.sock"
export TESTCONTAINERS_RYUK_DISABLED="true"

# Now run tests normally
uv run pytest -m integration
```

### Code Quality

```bash
# Format code
uv run ruff format

# Lint code
uv run ruff check
```

### Docker Environment

For development and testing, use the included Docker Compose setup:

```bash
# Start ClickHouse
docker compose up -d

# Stop ClickHouse
docker compose down
```
