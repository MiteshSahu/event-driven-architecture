# Mini Data Platform: Interview Questions and Answers

This guide explains the project from basic to advanced level in simple words.
The answers describe what this learning project actually implements. Do not
claim production numbers such as 11.6 million records or a 20% improvement
unless you can explain how those numbers were measured in the real system.

## Level 1: Architecture fundamentals

### 1. What does this project do?

It is a small event-driven data platform. A registration service records CSV
files in PostgreSQL. Debezium detects those database changes and publishes them
to Kafka. A Java CSV collector consumes the file events, reads the files and
inserts orders. Debezium publishes the new orders, and another Java service
processes them. Finally, a Java Spark job creates analytical summaries, and
Airflow schedules that batch job.

```text
CSV registration -> PostgreSQL -> Debezium -> Kafka -> CSV collector
  -> PostgreSQL orders -> Debezium -> Kafka -> order processor
  -> processed_orders -> Spark -> daily_order_metrics
                              ^
                              |
                           Airflow
```

### 2. Why was the application divided into microservices?

Each service has one clear responsibility:

- `file-registration` registers work but does not process CSV contents.
- `csv-collector` processes files and creates orders.
- `order-processor` handles individual order events.
- Spark performs batch analytics.
- Airflow schedules and monitors batch work.

This separation lets each part be deployed, scaled, monitored and repaired
independently. The trade-off is more network communication and operational
complexity.

### 3. What is the difference between synchronous and asynchronous processing?

In synchronous processing, the caller waits for the work to finish. In this
project, registration returns after committing the file records; it does not
wait for every file and order to finish. The remaining work is asynchronous:
Debezium creates Kafka events and consumers process them later.

This improves decoupling and throughput, but the API must report an accepted or
registered state rather than falsely claiming that all downstream work has
completed.

### 4. What is event-driven architecture?

An event-driven system reacts to facts that have already happened, such as
“source file registered” or “order created.” Producers do not directly call all
downstream consumers. They publish events, and interested consumers react.

### 5. What is the difference between operational and analytical data?

Operational data answers questions about individual transactions, such as the
state of one order. Analytical data summarizes many transactions, such as daily
order count and revenue. `processed_orders` is operational history;
`daily_order_metrics` is analytical output.

## Level 2: Docker and containerization

### 6. Why is every component containerized?

Containers give every developer the same Java, Kafka, Spark, Airflow and
database versions. The host only needs Docker. Containers also create isolated
processes and a shared Docker network where services can use names such as
`postgres` and `kafka`.

### 7. What is the difference between an image and a container?

An image is an immutable template containing an application and its runtime. A
container is a running instance of that image. We build one Spark image, but
every Spark execution creates a new temporary container from it.

### 8. Why is Spark not a permanently running container here?

Spark is used as a batch job. Docker starts the `spark-analytics` container,
runs `spark-submit`, writes the result and removes the container. PostgreSQL
keeps the output. This is simpler than keeping an idle Spark service alive.

### 9. What is the difference between `./platform down` and `reset`?

`down` stops and removes containers while preserving named volumes. `reset`
also removes PostgreSQL, Kafka and Airflow volumes, giving the next experiment
a clean state. A clean reset makes issue-versus-fix comparisons repeatable.

### 10. How do containers communicate?

Docker Compose puts them on the same network. Inside containers, `localhost`
means the current container, so the Spark job connects to
`jdbc:postgresql://postgres:5432/orders`, not `localhost:5432`.

## Level 3: PostgreSQL CDC and Debezium

### 11. What is CDC?

Change Data Capture converts database inserts, updates and deletes into events.
Instead of repeatedly querying a table for changes, Debezium reads PostgreSQL’s
write-ahead log and publishes ordered change records to Kafka.

### 12. Where is the Debezium code that creates events?

There is no custom Java producer for CDC events. Debezium is configured through
`infrastructure/debezium/postgres-connector.json`. PostgreSQL writes changes to
its WAL; the Debezium connector reads the WAL and acts as the Kafka producer.

### 13. Why use CDC instead of publishing to Kafka after a database insert?

A normal “database insert, then Kafka publish” flow has a dual-write problem.
The database commit may succeed while Kafka publishing fails, leaving the two
systems inconsistent. CDC publishes from the committed database log, so only
committed changes become events. Another common solution is the transactional
outbox pattern.

### 14. Does CDC guarantee that an event is processed exactly once?

No. CDC reliably captures committed changes, but consumers may receive a
record again after a crash or retry. Consumers still need idempotency. The
order processor records Kafka topic, partition and offset in
`processed_events`, whose primary key prevents the same event from producing
the business effect twice.

## Level 4: Kafka fundamentals

### 15. What are a Kafka topic, partition and offset?

A topic is a named event stream. A partition is an ordered section of that
stream. An offset is a record’s position inside one partition. Kafka guarantees
ordering within a partition, not across the entire topic.

### 16. Why does the Kafka record key matter?

Kafka hashes the key to select a partition. Events with the same key normally
go to the same partition and preserve relative order. A poor key can send most
records to one partition, creating a hot partition and an overloaded consumer.

### 17. What is a consumer group?

Consumers with the same group ID cooperate. Kafka assigns each partition to at
most one active consumer in that group. Different groups can independently
read the same topic.

### 18. Why can four consumers fail to provide four-way parallelism?

Parallelism is limited by partitions and data distribution. Four consumers
cannot help if the topic has one partition. They also cannot balance useful
work if all keys hash to one of four partitions; one consumer gets the hot
partition while the others are mostly idle.

### 19. What do current offset, log-end offset and lag mean?

- Current offset: how far the consumer group has committed.
- Log-end offset: the newest offset currently in the partition.
- Lag: log-end offset minus current offset.

Lag means records are waiting, but it does not by itself explain why. Possible
causes include slow processing, blocked code, a hot partition, failures or an
unavailable consumer.

### 20. What is a Kafka rebalance?

A rebalance changes partition ownership when consumers join, leave, crash or
when topic partitions change. During a rebalance, processing may pause. Long
blocking work in a listener can also cause Kafka to consider a consumer dead if
it stops polling within the configured interval.

## Level 5: Partition-aware processing and load balancing

### 21. How did this project reproduce one overloaded pod?

The skewed routing strategy uses one `batchId` as the routing key for all files
in the batch. Debezium uses that column as the Kafka key, so all records hash to
one partition. Kafka correctly assigns that partition to one collector pod,
which consequently performs nearly all the work.

### 22. How was the hot-partition problem fixed?

The balanced strategy uses `fileId` as the routing key. Different file IDs
spread across Kafka partitions, allowing multiple consumer pods to process the
batch. The consumer group still supplies partition assignment; our framework
supplies a workload-aware key and bounded processing behavior.

### 23. If Kafka already balances partitions, what did the custom framework add?

Kafka balances partition ownership, not business workload. It does not choose
the application key, validate whether keys create skew, manage the worker
executor inside a listener, enforce a bounded queue, define terminal failure
behavior or expose application-specific metrics. The project adds those
responsibilities around Kafka’s built-in group coordination.

### 24. How should you describe “partition-aware framework” honestly?

Say: “I designed routing strategies that control the CDC event key, configured
consumer concurrency around topic partition count, used a bounded worker
executor for backpressure, preserved per-partition Kafka semantics, and exposed
lag and per-pod distribution for verification.” Do not say that you replaced
Kafka’s partition assignment algorithm.

## Level 6: Java concurrency, latches and executors

### 25. What was the latch bug?

The unsafe collector waits for every file to signal completion. An invalid
filename takes an early-return path without calling `countDown()`. The invalid
file is already terminal, but the latch remains at one forever, so the batch
coordinator appears stuck.

### 26. How was the latch bug fixed?

Every terminal path must signal completion, including rejected files and
failures. The safe policy also uses a bounded wait instead of waiting forever.
This prevents one missing signal from blocking a pod indefinitely.

### 27. What is the difference between `AtomicInteger` and `CountDownLatch`?

`AtomicInteger` provides thread-safe numeric updates but does not define a
complete waiting protocol. `CountDownLatch` represents a fixed number of
completion signals and lets another thread wait until the count reaches zero.
Neither automatically prevents logical bugs such as a missing signal or
waiting for an exact counter value that has already been skipped.

### 28. Why use an executor framework?

Kafka listener threads should not create unlimited raw threads. An executor
controls worker count, queueing and shutdown. It separates polling from file
processing while providing a place to apply backpressure.

### 29. What is wrong with an unbounded executor queue?

When producers submit faster than workers finish, waiting tasks accumulate in
JVM memory. Kafka lag may look small because records were already handed to the
application, while the real backlog is hidden in an unbounded queue. Eventually
latency and memory usage can become severe.

### 30. How does the bounded executor provide backpressure?

The safe executor has a fixed worker count and an `ArrayBlockingQueue`. When
both are full, its caller-runs policy makes the Kafka listener execute the
overflow task. That slows polling naturally, leaving excess work visible as
Kafka lag instead of hiding it in JVM memory.

### 31. Does multithreading automatically improve throughput?

No. It helps when tasks can safely run in parallel and CPU, database, disk and
partitions can support the load. Too many threads increase context switching,
database connections and contention. Throughput must be measured while also
watching latency, errors, lag and resource use.

## Level 7: Delivery guarantees, retries and data loss

### 32. What do at-most-once, at-least-once and exactly-once mean?

- At-most-once may lose a record but does not retry it.
- At-least-once retries failures, so duplicates are possible.
- Exactly-once means one observable business effect, which normally requires
  coordination or idempotency across all involved systems.

This project aims for at-least-once consumption plus idempotent database
writes.

### 33. When should a Kafka offset be committed?

Commit only after the business operation succeeds or after the record reaches
an intentional terminal path such as a dead-letter topic. Committing before
processing can lose work after a crash. Retrying forever without a terminal
policy can block a partition.

### 34. What is a dead-letter topic?

A DLT stores records that still fail after configured retries. It prevents one
poison record from blocking all later records in the partition and preserves
the original event plus failure context for investigation and replay.

### 35. How could an unwanted Kafka header cause apparent data loss?

A deserializer, converter or listener may reject a record because a header has
an unexpected name, type or value. If error-handling code catches that exception
but commits the offset, Kafka considers the record processed even though the
business logic never ran. The safe design validates headers, retries transient
errors, sends permanent failures to a DLT with diagnostic headers, and commits
only after a successful or deliberate terminal outcome.

### 36. Is the unwanted-header experiment implemented already?

Not yet. It is the next planned controlled comparison. The project currently
has retry/DLT behavior for file-processing failures. In an interview, clearly
separate what has been implemented locally from what was observed or fixed in
the organizational system.

## Level 8: Spark with Java

### 37. Who calls Spark in this project?

For direct testing, `./platform run-spark` starts a temporary container whose
Docker `CMD` runs `spark-submit`. In the scheduled path, the Airflow DAG asks
Docker to start the same Spark image with the same `spark-submit` command.

### 38. What does the Java Spark job do?

It reads `processed_orders` using JDBC, rejects invalid records, normalizes
statuses, creates amount bands, caches enriched rows, groups them by date,
status and band, calculates count/sum/average/maximum/distinct customers, ranks
segments with a window function and writes `daily_order_metrics`.

### 39. How are Java Streams and Spark transformations different?

Java Streams process objects inside one JVM. Spark `Dataset` transformations
build a lazy execution plan that Spark can divide into stages and tasks across
executors. Their vocabulary is similar—filter, map-like projections and
aggregation—but their execution models are different.

### 40. What are Spark transformations and actions?

Transformations such as `filter`, `withColumn` and `groupBy` describe a new
Dataset lazily. Actions such as `count`, `show` and `write` cause Spark to
execute the plan. The job’s `count()` action materializes the cache before later
actions reuse it.

### 41. What is a Spark shuffle?

A shuffle moves data between partitions, usually for grouping, joining,
distinct counting, ranking or global sorting. In the physical plan, `Exchange`
represents this redistribution. Shuffles are expensive because they involve
serialization, memory, network and sometimes disk.

### 42. Why cache the enriched Dataset?

The enriched rows are used by multiple actions. Without caching, Spark may
reread PostgreSQL and repeat cleaning for each action. `MEMORY_AND_DISK` keeps
partitions in memory where possible and spills the rest to disk. Caching is not
always beneficial; it costs memory and should be used only for reused,
expensive data.

### 43. Is the current Spark job truly distributed?

It uses `local[*]`, so the driver and executor threads run in one temporary
container. Spark still builds stages, partitions and tasks, but it is not a
multi-machine cluster. A later lab can use a Spark master and multiple workers,
plus a partitioned JDBC read, to demonstrate distributed execution.

### 44. Why does the JDBC input currently show one partition?

The simple `read().jdbc(url, table, properties)` overload creates one JDBC
input partition. For parallel reading, Spark needs a suitable numeric or date
partition column, lower and upper bounds, and a requested partition count.
Those settings make Spark issue multiple non-overlapping database queries.

## Level 9: Airflow

### 45. What is Airflow’s responsibility?

Airflow schedules, orders, retries and records workflow tasks. It does not
replace Spark. Spark transforms data; Airflow decides when to start Spark and
records whether it succeeded.

### 46. What is a DAG?

A Directed Acyclic Graph is a workflow whose tasks have directional
dependencies and no circular dependency. This project has:

```text
submit_spark_job -> report_completion
```

The completion task cannot run unless Spark succeeds.

### 47. Why test Spark directly before testing it through Airflow?

Direct execution isolates transformation, JDBC and Spark problems. If direct
Spark succeeds but the DAG fails, investigate orchestration, Docker socket
permissions, image names or Airflow configuration. This reduces the debugging
surface.

### 48. What happens when the Spark container exits with a non-zero code?

The Docker client call raises an error, the Airflow task fails, the downstream
completion task does not run and the DAG run is marked failed. Retry behavior
can then be configured on the Airflow task for transient failures.

## Level 10: Kubernetes and production design

### 49. How would this Docker Compose project move to Kubernetes?

Spring services become Deployments and Services. Configuration moves to
ConfigMaps and Secrets. Health checks become readiness and liveness probes.
Kafka and PostgreSQL should normally use managed services or carefully designed
StatefulSets. Airflow can use its Helm chart, and Spark can submit driver and
executor pods through the Spark-on-Kubernetes scheduler.

### 50. What is the difference between scaling pods and increasing Kafka partitions?

More pods add consumer instances, but useful Kafka parallelism cannot exceed
the number of assigned partitions. Increasing partitions adds potential
parallelism but changes key distribution and does not fix a poor key with
extreme skew. Both partition count and pod count must be planned together.

### 51. What is the difference between readiness and liveness probes?

Readiness answers “should this pod receive traffic now?” Liveness answers “is
this process stuck badly enough to restart?” A service can be alive but not
ready, for example while Kafka or the database is unavailable. Application
health must also expose stuck business work because an HTTP health endpoint can
remain green while a worker is deadlocked.

### 52. How would you monitor this platform?

Monitor infrastructure and business flow together:

- Kafka consumer lag per partition and rebalance count
- message rate, retry rate and DLT count
- per-pod file distribution and processing duration
- executor active threads, queue size and caller-runs count
- JVM CPU, memory, garbage collection and thread state
- database connection pool and query latency
- Spark stage duration, shuffle read/write and failed tasks
- Airflow queued/running/failed DAGs and task duration

### 53. How would you make the system highly available?

Run multiple stateless service replicas across nodes, use replicated Kafka and
a highly available PostgreSQL setup, avoid local-only business state, use
idempotent consumers, set resource requests and limits, add disruption budgets,
and test failure recovery. High availability is an end-to-end property; adding
replicas to one service is not enough.

## Level 11: Scenario and resume questions

### 54. Explain the one-pod bottleneck using the STAR format.

**Situation:** A batch used one routing key, so its records landed in one Kafka
partition and one collector pod performed nearly all work.

**Task:** Make file processing use the available partitions and pods without
breaking consumer-group semantics.

**Action:** I compared per-partition lag and per-pod distribution, separated
skewed batch routing from balanced file routing, used file-level keys, and
verified the result with three collectors and four partitions.

**Result:** The controlled local scenario changes from approximately `60/0/0`
work distribution to work across all three pods. Production throughput numbers
must come from production measurements, not this local lab.

### 55. Explain the stuck collector issue using the STAR format.

**Situation:** An invalid filename exited before signaling a batch latch, while
the service health endpoint remained up.

**Task:** Prevent one rejected file from blocking batch completion forever.

**Action:** I reproduced the missing completion signal, moved signaling into
every terminal path, added bounded waiting, retries/DLT for processing failures,
and exposed batch progress fields such as remaining latch count.

**Result:** The unsafe scenario remains `STUCK` with latch count one; the safe
scenario finishes with rejections and latch count zero.

### 56. How would you defend “improving parallel processing efficiency”?

Explain the mechanisms and evidence rather than using a vague claim: key
selection distributed work across partitions; multiple group consumers used
those partitions; a bounded executor prevented hidden backlog; and metrics
showed partition lag and per-pod work distribution. If quoting a percentage,
define the exact before/after workload, duration and formula.

### 57. How would you calculate a data-loss reduction percentage?

Define a controlled failure workload and count events that never produce either
a successful business effect or a DLT record:

```text
loss rate = unexplained missing events / input events * 100
reduction = (old loss rate - new loss rate) / old loss rate * 100
```

Retries, DLT count and duplicate suppression must be included. Do not call a
DLT record “lost”; it is failed but preserved for investigation.

### 58. What are the current project limitations?

- Spark runs locally in one container rather than on multiple workers.
- The happy fixture is intentionally tiny.
- PostgreSQL is both operational source and analytical sink.
- Airflow uses its standalone learning configuration.
- Kafka and PostgreSQL each run as a single local instance.
- The unwanted-header loss scenario is planned but not implemented.
- Kubernetes manifests and a Spark-on-Kubernetes lab are still future steps.

Calling out limitations demonstrates engineering judgment and prevents a
learning project from being presented as a production platform.

## Quick commands to demonstrate during an interview

```bash
# End-to-end event-driven happy path
./platform scenario happy

# Missing latch signal versus fix
./platform scenario latch-unsafe
./platform scenario latch-safe

# One overloaded pod versus balanced routing
./platform scenario hot-partition
./platform scenario balanced

# Unbounded queue versus bounded backpressure
./platform scenario executor-unbounded
./platform scenario executor-bounded

# Retry and dead-letter topic
./platform scenario retry-dlt

# Java Spark directly
./platform scenario spark

# Airflow orchestrates the Java Spark JAR
./platform scenario airflow-spark
./platform airflow-runs
./platform analytics-results

# Stop everything
./platform down
```
