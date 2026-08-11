# Single-threaded consumer baseline

## Configuration

- Generated records: 1,000
- Kafka topic partitions: 4
- Consumer group: `order-processors`
- Consumer instances: 1
- Spring Kafka listener concurrency: 1
- Simulated business processing delay: 25 ms per record

## Commands

```bash
./platform reset
./platform up
./platform generate 1000
./platform ingest-generated
./platform watch-lag
```

## Observed run

The ingestion service accepted all 1,000 rows with zero rejected rows. During
the run, total consumer lag rose while Debezium was still publishing the
committed transaction, peaked at 887 in the captured samples, and then drained:

| Sample | Total lag | Processed rows |
|---:|---:|---:|
| 1 | 501 | 34 |
| 2 | 887 | 128 |
| 3 | 793 | 221 |
| 4 | 703 | 311 |
| 5 | 609 | 405 |
| 6 | 517 | 497 |
| 7 | 426 | 587 |
| 8 | 335 | 680 |
| 9 | 241 | 774 |
| 10 | 141 | 873 |
| 11 | 45 | 970 |
| 12 | 0 | 1,000 |

## Interpretation

Four Kafka partitions were available, but one listener thread processed all of
them sequentially. Kafka could accept CDC events faster than the application
could complete its simulated work, so lag accumulated. The group eventually
reached zero lag, proving that no records remained waiting in Kafka.

This is the baseline that will be compared with partition-aware listener
concurrency and multiple consumer instances. The same record count and delay
must be used for a fair comparison.

