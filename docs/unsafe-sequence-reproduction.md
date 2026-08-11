# Unsafe sequence coordination reproduction

## Purpose

This lab reproduces an application-level lost-progress condition. It is not a
Kafka broker failure and is intentionally unsafe.

## Configuration

- Records: 100
- Kafka partitions: 4
- Consumers/listener threads: 4
- Processing mode: `unsafe-sequence`
- Intentional fault: after sequence 49, advance the shared counter from 50 to 51

## Command

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

## Observed evidence

After the controlled fault:

```text
next expected sequence: 51
completed sequences: 49
waiting consumer threads: 4
fault triggered: true
custom stuck status: true
Spring health status: UP
source records: 100
processed records: 49
processed-event records: 49
total Kafka lag: 51
```

The per-partition lag was 16, 15, 16, and 4. A JVM thread dump showed all four
Spring Kafka listener threads in `WAITING (parking)` at
`UnsafeSequenceCoordinator.awaitTurn()`.

Sequence 50 was waiting for exact equality with 50, but the shared counter had
already advanced to 51. Because the counter only advances, that predicate could
never become true. Other listener threads were also waiting for later turns, so
the entire consumer group stopped making useful progress.

## Conclusion

Kafka correctly assigned one partition to each consumer and retained all
unprocessed events. The failure occurred after delivery, inside application
coordination. A normal HTTP health check remained green, demonstrating that
container liveness alone does not prove pipeline progress.

