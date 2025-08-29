CREATE TABLE IF NOT EXISTS user (
    first_name String,
    last_name String,
    timestamp DateTime,
    platform String
)
ENGINE = MergeTree
ORDER BY (toDate(timestamp), platform);
