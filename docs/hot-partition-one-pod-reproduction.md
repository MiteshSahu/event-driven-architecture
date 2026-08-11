# Hot-partition / one-pod reproduction

## Cause

Three `csv-collector` containers join the same `csv-collectors` consumer group.
Debezium is configured to use `collector_run_id` as the message key for
`source_files`. All files in one batch share that value, so Kafka hashes every
event in the batch to the same partition.

Kafka assigns a partition to only one consumer within a group. It cannot divide
the records of that hot partition among the other collector pods.

## Run

```bash
./platform reset
COLLECTOR_REPLICAS=3 \
COLLECTOR_MODE=safe \
COLLECTOR_PROCESSING_DELAY_MS=100 \
COLLECTOR_TIMEOUT_SECONDS=20 \
./platform up

./platform register-skewed-batch 60
sleep 1
./platform collector-group
./platform collector-distribution
sleep 8
./platform collector-distribution
./platform collector-group
```

## Measured result

During processing:

```text
partition 3: current offset 13, end offset 82, lag 69
partition 0: no records
partition 1: no records
partition 2: no records
```

Early database distribution:

```text
pod 0cbe444e3376: COMPLETED 17
UNPROCESSED: PENDING 43
```

Final distribution:

```text
csv-collector-1: 60 received file events, 60 completed files
csv-collector-2: 0 received file events
csv-collector-3: 0 received file events
```

Final consumer lag was zero, but only after one pod processed the entire batch.
Adding replicas provided no throughput benefit for this hot key.

## Conclusion

Kafka balances partitions, not individual messages. More pods help only when
work is distributed across enough partitions.

## Balanced fix

Debezium now uses an explicit `routing_key` column as the Kafka key. The two
registration endpoints control the experiment:

- `/batches/skewed` gives all files the same batch-level routing key.
- `/batches/balanced` gives every file its own routing key.

Run the balanced workload after a reset:

```bash
./platform reset
COLLECTOR_REPLICAS=3 \
COLLECTOR_MODE=safe \
COLLECTOR_PROCESSING_DELAY_MS=100 \
COLLECTOR_TIMEOUT_SECONDS=30 \
./platform up

./platform register-balanced-batch 60
sleep 1
./platform collector-group
./platform collector-distribution
sleep 4
./platform collector-distribution
```

Measured comparison:

| Measurement | Skewed key | File-level key |
| --- | ---: | ---: |
| Active Kafka partitions | 1 | 4 |
| Active collector pods | 1 | 3 |
| Pod file distribution | 60 / 0 / 0 | 31 / 19 / 10 |
| First-to-last file receipt | 6.775 s | 2.224 s |
| Final Kafka lag | 0 | 0 |

The file-level strategy reduced the measured file-receipt/processing window by
about 67%. It did not create perfect equality: four partitions across three
pods means one pod owns two partitions, and random keys do not guarantee equal
record counts. The important result is that all partitions and pods performed
useful work.
