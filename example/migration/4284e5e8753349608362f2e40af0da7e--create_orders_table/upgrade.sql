CREATE TABLE IF NOT EXISTS orders (
    order_id UInt64,
    user_id UInt64,
    product_name String,
    quantity UInt32,
    price Decimal(10, 2),
    order_date DateTime,
    status Enum8('pending' = 1, 'confirmed' = 2, 'shipped' = 3, 'delivered' = 4, 'cancelled' = 5),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, user_id)
SETTINGS index_granularity = 8192;
