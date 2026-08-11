# Database-backed CSV collector lab

## Architecture

```text
registration request
  -> PostgreSQL source_files (PENDING)
  -> CSV collector claims rows
  -> parallel file workers read /data/file-batch
  -> valid CSV rows inserted into PostgreSQL orders
  -> Debezium captures orders changes
  -> Kafka orders-cdc.public.orders
  -> Java order-processor consumer
  -> processed_orders
```

The collector receives registrations through HTTP only to create a small local
source system. Before starting workers, it writes all registrations to
`source_files` and reads the work back from PostgreSQL. The worker inputs are
therefore database records, not uploaded multipart file contents.

## Unsafe reproduction

```bash
./platform reset
./platform up
./platform test-collector unsafe
sleep 12
./platform collector-progress unsafe
./platform collector-files unsafe
./platform processed
./platform consumer-groups
```

Measured result:

```text
collector status       STUCK
submitted files        3
successful files       2
invalid files          1
completion signals     2
remaining latch count  1
processed Kafka events 2
Kafka lag              0
```

The invalid database work item is marked `INVALID`, but the intentionally
broken worker returns without signalling the latch. Two valid files still flow
through PostgreSQL, Debezium, Kafka, and the consumer. The batch coordinator,
however, can never finish.

## Safe comparison

Use a clean database because the fixture order IDs are intentionally stable:

```bash
./platform reset
./platform up
./platform test-collector safe
sleep 4
./platform collector-progress safe
./platform collector-files safe
./platform processed
./platform consumer-groups
```

Measured result:

```text
collector status       COMPLETED_WITH_REJECTIONS
submitted files        3
successful files       2
invalid files          1
completion signals     3
remaining latch count  0
processed Kafka events 2
Kafka lag              0
```

The safe worker signals from `finally`, and its coordinator has a bounded wait.
Rejecting an invalid file remains a business outcome; it no longer prevents
the collector run from reaching a terminal state.
