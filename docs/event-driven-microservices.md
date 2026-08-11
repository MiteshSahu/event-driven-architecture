# Event-driven CSV microservices

## Runtime services

| Service | Responsibility | Port |
| --- | --- | ---: |
| `file-registration` | Commits file metadata to `source_files` | 8081 |
| Debezium Connect | Converts PostgreSQL WAL changes into Kafka events | 8083 |
| `csv-collector` | Consumes source-file events and processes CSV files | 8084 |
| `order-processor` | Consumes order events and persists results | 8082 |
| Kafka UI | Shows topics, partitions, messages, and groups | 8080 |

## Event flow

```text
POST /api/source-files/batches
  -> file-registration commits source_files rows
  -> PostgreSQL WAL -> Debezium -> orders-cdc.public.source_files
  -> SourceFileCdcListener in csv-collector
  -> CSV worker pool -> orders rows
  -> PostgreSQL WAL -> Debezium -> orders-cdc.public.orders
  -> OrderCdcListener in order-processor -> processed_orders
```

There is no HTTP call from `file-registration` to `csv-collector`. The database
commit and CDC event are the boundary. Collector status updates also produce
CDC events, so the listener accepts only inserts with Debezium operation `c`
and status `PENDING`.

Each source row carries the same `collector_run_id` and
`expected_file_count`, allowing the collector to reconstruct a batch from
individually delivered Kafka events.

## Safe run

```bash
./platform reset
COLLECTOR_MODE=safe ./platform up
./platform register-files
sleep 4
./platform collector-progress
./platform source-files
./platform collector-group
./platform processed
./platform consumer-groups
```

Expected: two successful files, one invalid file, three completion signals,
latch count zero, and `COMPLETED_WITH_REJECTIONS`.

## Unsafe reproduction

```bash
./platform reset
COLLECTOR_MODE=unsafe ./platform up
./platform register-files
sleep 12
./platform collector-progress
./platform source-files
curl -fsS http://localhost:8084/actuator/health
```

Expected: two successful files, one invalid file, two completion signals,
latch count one, and `STUCK`, while health remains `UP`.

## Acknowledgement and crash recovery

The listener dispatches work to the bounded worker pool but waits for its
future on the Kafka listener thread. It acknowledges the offset only after the
file reaches a terminal database result. If the container dies during work,
the uncommitted offset is eligible for redelivery.

Run the controlled crash experiment:

```bash
./platform reset
COLLECTOR_MODE=safe \
COLLECTOR_PROCESSING_DELAY_MS=10000 \
COLLECTOR_TIMEOUT_SECONDS=30 \
./platform up

./platform register-files
sleep 2
./platform source-files
./platform crash-collector
./platform status
./platform start-collector

# SIGKILL cannot leave the consumer group gracefully. Allow Kafka's default
# 45-second session timeout, reassignment, and the configured processing delay.
sleep 60

./platform collector-group
./platform source-files
./platform collector-progress
./platform processed
```

Verified behavior on 2026-08-11:

```text
before crash: all three source rows PENDING
after restart: Kafka redelivered all three source-file inserts
collector: COMPLETED_WITH_REJECTIONS, latch count 0
source rows: two COMPLETED, one INVALID
processed orders: 2
collector Kafka lag: 0
```

The 45-second pause is broker failure detection, not file processing. A
graceful shutdown sends a leave-group request and rebalances faster. Production
systems tune session/static-membership settings based on recovery-time and
rebalance tradeoffs.

## Retry and dead-letter topic

Use a batch containing one valid file and one UUID-valid filename whose path
does not exist:

```bash
./platform reset
COLLECTOR_MODE=safe ./platform up
./platform register-failure-batch
sleep 5
./platform collector-progress
./platform source-files
./platform dlt-messages 1
./platform processed
./platform collector-group
```

The missing file is attempted three times with a bounded delay. After the last
failure, the listener publishes the original Debezium event to
`source-files.DLT`, marks its database row `FAILED`, signals the batch latch,
and acknowledges the original source-topic offset. The valid file is unaffected.

Verified result:

```text
successful files       1
failed files           1
completion signals     2
remaining latch count  0
batch status           COMPLETED_WITH_REJECTIONS
DLT messages           1
downstream orders      1
source consumer lag    0
```

The file-processing transaction uses `rollbackFor = Exception.class` because
file I/O throws checked `IOException`. Spring's default transactional behavior
rolls back for unchecked exceptions, but not every checked exception. Without
the explicit rule, the claim could be committed as `PROCESSING` even though
the file read failed, causing a retry to misclassify the row.
