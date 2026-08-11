# Four-thread partition-aware comparison

## Controlled variables

The parallel run kept the baseline workload unchanged:

- Generated records: 1,000
- Kafka partitions: 4
- Simulated work: 25 ms per record
- Consumer group: `order-processors`

Only listener concurrency changed from `1` to `4`.

## Command

```bash
./platform reset
CONSUMER_CONCURRENCY=4 PROCESSING_DELAY_MS=25 ./platform up
./platform generate 1000
./platform ingest-generated
./platform watch-lag
```

## Partition assignment

Kafka assigned one partition to each consumer created inside the same
order-processor container:

| Partition | Consumer client |
|---:|---|
| 0 | `consumer-order-processors-1` |
| 1 | `consumer-order-processors-2` |
| 2 | `consumer-order-processors-3` |
| 3 | `consumer-order-processors-4` |

Four distinct Spring Kafka listener thread names appeared in the application
logs, confirming that processing occurred concurrently.

## Observed result

| Metric | One thread | Four threads |
|---|---:|---:|
| Source records | 1,000 | 1,000 |
| Processed records | 1,000 | 1,000 |
| Final lag | 0 | 0 |
| Approximate processing window | about 25 s | about 9.3 s |

The four-thread run's captured lag samples were `952`, `684`, `408`, `132`,
and `0`. All source, processed-order, and processed-event counts reconciled at
1,000.

The improvement is not exactly four times because the 25 ms simulated work is
not the entire cost. JSON parsing, database transactions, Kafka offset commits,
logging, CDC publication, and local container resource contention remain.

## Conclusion

Kafka partitions define the maximum useful consumer parallelism. Configuring
four consumers allowed all four partitions to make progress simultaneously.
The experiment improved throughput without changing the workload or losing
records.

