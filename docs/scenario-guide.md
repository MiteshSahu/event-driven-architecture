# Repeatable scenario guide

Every scenario resets the database and Kafka volumes, starts the required
containers, submits its fixture, waits for the expected observation window,
and prints evidence. Stop afterward with `./platform down`.

| Goal | Command | Expected result |
| --- | --- | --- |
| Entire happy path | `./platform scenario happy` | Two valid files complete and two orders reach the downstream consumer |
| Reproduce missing latch signal | `./platform scenario latch-unsafe` | `STUCK`, latch count 1, health `UP` |
| Verify latch fix | `./platform scenario latch-safe` | `COMPLETED_WITH_REJECTIONS`, latch count 0 |
| Reproduce one busy pod | `./platform scenario hot-partition` | Three replicas, distribution approximately `60 / 0 / 0` |
| Verify partition fix | `./platform scenario balanced` | All four partitions and all three pods perform work |
| Verify retry and DLT | `./platform scenario retry-dlt` | Missing file retried three times, marked `FAILED`, original event in DLT |
| Reproduce executor queue risk | `./platform scenario executor-unbounded` | Queue accepts waiting tasks with no capacity limit and `callerRuns=0` |
| Verify executor backpressure | `./platform scenario executor-bounded` | Queue stops at configured capacity and `callerRuns` increases |
| Run batch analytics directly | `./platform scenario spark` | Spark writes daily counts and totals to `daily_order_metrics` |
| Orchestrate Spark with Airflow | `./platform scenario airflow-spark` | Airflow launches the Spark container and records both DAG tasks |

## Code comparisons

### Latch issue versus fix

- `UnsafeCollectorCompletionPolicy` deliberately skips the invalid-file signal
  and waits without a timeout.
- `SafeCollectorCompletionPolicy` signals every terminal outcome and performs a
  bounded wait.
- `EventDrivenCollector` contains the shared worker/retry logic and delegates
  only the behavior being compared to the selected policy.

Select the mode at container startup:

```bash
COLLECTOR_MODE=unsafe ./platform up
COLLECTOR_MODE=safe ./platform up
```

### Hot partition versus balanced routing

- `SkewedBatchRoutingStrategy` returns `batchId`, giving all batch files one key.
- `BalancedFileRoutingStrategy` returns `fileId`, allowing independent hashes.
- Debezium reads the resulting `routing_key` column as the Kafka record key.

The two strategies change only routing. File count, consumer group, partitions,
replicas, processing delay, worker code, and database remain the same, making
the experiment a controlled comparison.

### Executor queue issue versus backpressure

- `UnboundedCollectorWorkerConfiguration` uses
  `Executors.newFixedThreadPool`, which internally uses an unbounded queue.
- `BoundedCollectorWorkerConfiguration` uses `ThreadPoolExecutor` with an
  `ArrayBlockingQueue` and a caller-runs rejection handler.
- `ExecutorStatusController` exposes active threads, queued tasks, remaining
  capacity, completed tasks, and caller-runs count.

Measured with one worker, four Kafka listener threads, 60 files, and 200 ms of
simulated work:

```text
unbounded: active=1, queued=3, capacity=unbounded, callerRuns=0
bounded:   active=1, queued=1, remainingCapacity=0, callerRuns=2
```

`callerRuns=2` means two Kafka listener threads executed overflow tasks. Their
polling slowed naturally, keeping overflow in Kafka lag instead of allowing an
unbounded JVM queue to grow.

### Spark versus Airflow responsibilities

- `analytics/spark/src/main/java/dev/learning/platform/analytics/DailyOrderAnalytics.java`
  owns the data transformation.
- `analytics/airflow/dags/daily_order_analytics.py` owns the schedule and job
  lifecycle.
- Run `./platform scenario spark` to isolate Spark, then
  `./platform scenario airflow-spark` to verify orchestration.

See `docs/spark-airflow.md` for the complete flow and verification commands.
