# Mini Distributed Data Platform

An intentionally small learning project for following one data change through
Spring Boot, PostgreSQL CDC, Debezium, Kafka, Java consumers, Spark, Airflow,
Docker, and Kubernetes.

## Current event-driven architecture

The primary flow is now split into independent Spring Boot microservices:

```text
file-registration -> PostgreSQL source_files -> Debezium -> Kafka
  -> csv-collector -> PostgreSQL orders -> Debezium -> Kafka
  -> order-processor -> processed_orders
```

Start it in safe mode and register the fixture batch:

```bash
./platform reset
COLLECTOR_MODE=safe ./platform up
./platform register-files
sleep 4
./platform collector-progress
./platform source-files
./platform processed
```

See `docs/event-driven-microservices.md` for the architecture, unsafe
reproduction, acknowledgement behavior, crash recovery, and Kafka
consumer-group commands. It also contains the retry and dead-letter-topic lab.

The one-pod hot-partition reproduction is documented in
`docs/hot-partition-one-pod-reproduction.md`. It starts three collector replicas
and compares a deliberately skewed batch key with balanced file-level routing
keys using the same 60-file workload.

## Milestone 1: CSV to Kafka through database CDC

The current vertical slice is:

```text
CSV -> Spring Boot ingestion -> PostgreSQL WAL -> Debezium -> Kafka -> Java order processor
```

All runtime components are containers. Maven and Java are not required on the
host.

## Commands

For one-command experiments, use:

```bash
./platform scenario happy
./platform scenario latch-unsafe
./platform scenario latch-safe
./platform scenario hot-partition
./platform scenario balanced
./platform scenario retry-dlt
./platform scenario executor-unbounded
./platform scenario executor-bounded
./platform scenario spark
./platform scenario airflow-spark
```

See `docs/scenario-guide.md` for expected output and side-by-side code paths.

```bash
# Build and start everything
./platform up

# Inspect container state
./platform status

# Follow all logs, or one service's logs
./platform logs
./platform logs csv-ingestion

# Upload the included three-record CSV
./platform test-csv

# Generate and upload a larger CSV using one-off containers
./platform generate 1000
./platform ingest-generated

# View rows written by the Kafka consumer
./platform processed

# Inspect partition assignment, offsets and consumer lag
./platform consumer-groups

# Watch lag every two seconds; exit with Ctrl+C
./platform watch-lag

# Stop containers while preserving Kafka/PostgreSQL data
./platform down

# Stop containers and delete learning data for a clean rerun
./platform reset
```

## Useful URLs

- Kafka UI: http://localhost:8080
- CSV ingestion API: http://localhost:8081/api/ingestions
- Invalid-file batch lab: http://localhost:8081/api/batch-lab/jobs/latest
- Processed orders API: http://localhost:8082/api/processed-orders
- Consumer progress API: http://localhost:8082/api/consumer-progress
- Spring health: http://localhost:8081/actuator/health
- Kafka Connect API: http://localhost:8083/connectors
- Airflow UI: http://localhost:8085 (`admin` / `admin`)

The Spark/Airflow batch analytics milestone is explained in
`docs/spark-airflow.md`. Spark reads the event-processing result from
`processed_orders`; Airflow schedules and monitors the disposable Spark job.

After `./platform test-csv`, open Kafka UI and inspect the
`orders-cdc.public.orders` topic. Debezium creates one change event for each
committed row inserted into `public.orders`. The `order-processor` consumes
those events as consumer group `order-processors` and writes `processed_orders`.

## Consumer baseline

The first consumer is deliberately configured with `concurrency: 1`. Its logs
print the Kafka partition, offset, CDC operation and Java thread for every
record. This gives us a measurable single-threaded baseline before introducing
partition-aware multithreading and multiple replicas.

It also sleeps for `25ms` per record by default to represent business work. Set
`PROCESSING_DELAY_MS` before `./platform up` to change it. For example,
`PROCESSING_DELAY_MS=100 ./platform up` makes the bottleneck more obvious.

Listener concurrency is configurable in the same way. The baseline uses one
consumer thread. `CONSUMER_CONCURRENCY=4 ./platform up` creates four consumers
inside the order-processor container, allowing the four topic partitions to be
processed concurrently.

## Baseline lag experiment

```bash
./platform reset
./platform up
./platform generate 1000

# Run this in terminal A
./platform watch-lag

# Run this in terminal B
./platform ingest-generated
```

Lag should rise after Debezium publishes the committed CSV transaction and then
fall toward zero as the one listener thread processes records. Press `Ctrl+C`
in terminal A to stop watching; containers continue running.

For a controlled parallel comparison, reset and repeat with:

```bash
CONSUMER_CONCURRENCY=4 PROCESSING_DELAY_MS=25 ./platform up
```

## Unsafe sequence lab

The optional `unsafe-sequence` mode deliberately reproduces an application
coordination bug. After sequence 49 completes, the shared counter skips turn 50
and advances to 51. A record waiting for exact equality with 50 can therefore
never proceed.

```bash
./platform reset
PROCESSING_MODE=unsafe-sequence \
CONSUMER_CONCURRENCY=4 \
PROCESSING_DELAY_MS=0 \
UNSAFE_SKIP_AFTER_SEQUENCE=49 \
./platform up
./platform generate 100
./platform ingest-generated
```

Inspect `./platform progress`, `./platform consumer-groups`, and
`./platform logs order-processor`. This mode is intentionally incorrect and is
only for the controlled learning experiment.

## Invalid-file latch lab

This closer reproduction assumes that a valid filename starts with a UUID. Two
UUID-prefixed CSV files signal completion; `invalid-orders.csv` is rejected on
an unsafe early-return path that deliberately omits `countDown()`.

```bash
./platform reset
./platform up
./platform test-invalid-batch
sleep 12
./platform batch-progress
```

The service remains healthy, while the batch reports two completion signals,
one invalid file, one remaining latch count, and `STUCK` status.

Run the same three files through the fixed path:

```bash
./platform test-safe-batch
./platform safe-batch-progress
```

The invalid file is still rejected, but the `finally` block supplies all three
completion signals. The safe job finishes as `COMPLETED_WITH_REJECTIONS` with a
remaining latch count of zero. Its coordinator also uses a five-second bounded
wait so an unexpected worker failure cannot block it forever.

## Database-backed CSV collector (complete pipeline)

This is the more realistic version of the invalid-file experiment. The trigger
first registers three work items in PostgreSQL `source_files`. The collector
queries those records, processes their mounted CSV paths concurrently, and
inserts valid rows into `orders`. Debezium then publishes the changes to Kafka,
where `order-processor` consumes them.

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

Replace `unsafe` with `safe` after another reset to verify that all three work
items reach terminal states and the latch reaches zero. See
`docs/database-backed-csv-collector.md` for the observed comparison.

`./platform up` waits until health checks pass before returning. The
`consumer-groups` command also allows a short registration window because a
Kafka consumer group exists only after its consumer has joined the broker.

## Stop versus reset

`./platform down` is the normal way to close the project. Named Docker volumes
preserve the database and Kafka state, so `./platform up` resumes the same
environment later. `./platform reset` deliberately deletes those volumes and
starts the experiment from empty storage on the next run.

## Learning note

The CSV service currently inserts one row at a time inside one transaction.
That is intentional: it is our measurable baseline bottleneck. A later
milestone will introduce chunking and batch inserts, and compare the results.
