CREATE TABLE IF NOT EXISTS processed_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64),
    product_id VARCHAR(64),
    amount NUMERIC(12, 2),
    status VARCHAR(32),
    source_operation VARCHAR(8) NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_events (
    topic VARCHAR(255) NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    operation VARCHAR(8) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic, partition_id, offset_id)
);

