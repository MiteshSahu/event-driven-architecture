# Spark batch analytics orchestrated by Airflow

This milestone adds a batch analytics path after the event-driven path. Kafka
and the Java services continue handling individual events. Spark periodically
reads the accumulated `processed_orders` table and computes daily totals.
Airflow decides when that Spark batch should run.

```text
CSV files
  -> file-registration -> PostgreSQL source_files
  -> Debezium -> Kafka -> csv-collector -> PostgreSQL orders
  -> Debezium -> Kafka -> order-processor -> PostgreSQL processed_orders
                                             |
                                             | scheduled batch
                                             v
Airflow DAG -> temporary Spark container -> daily_order_metrics
```

The responsibilities are intentionally separate:

- `DailyOrderAnalytics.java` contains cleaning, enrichment, caching,
  aggregation, window ranking, and JDBC output.
- `daily_order_analytics.py` contains orchestration only: schedule the work,
  start the Spark container, and record task success or failure.
- Docker gives each run a repeatable Spark runtime. The temporary Spark
  container is removed after the job, while PostgreSQL retains the result.

## Run Spark directly

```bash
./platform scenario spark
```

Expected result for the happy fixture:

```text
metric_date | status  | amount_band | order_count | total_amount | revenue_rank
------------+---------+-------------+-------------+--------------+-------------
today       | CREATED | MEDIUM      | 1           | 200.00       | 1
today       | CREATED | LOW         | 1           | 100.00       | 2
```

This command is useful when developing or debugging the transformation because
it removes Airflow from the path.

The console output is divided into three Java Spark learning checkpoints:

1. **Source** shows the application, local master, JDBC partition count,
   validation, enrichment, and the cache-materializing `count()` action.
2. **Physical plan** shows `InMemoryTableScan`, `Exchange`, aggregation,
   window ranking, and sorting.
3. **Analytical result** shows amount bands, aggregate measures, daily revenue
   rank, and the rows written through JDBC.

## Run Spark through Airflow

```bash
./platform scenario airflow-spark
./platform airflow-tasks
./platform airflow-runs
./platform analytics-results
```

Open <http://localhost:8085> to inspect the DAG and its two task states. The
local learning login is `admin` / `admin`. Follow logs with:

```bash
./platform logs airflow
```

Exit log following with `Ctrl+C`; the containers keep running. Stop all project
containers together with `./platform down`.

## Why both direct and orchestrated commands?

Spark is the processing engine; Airflow is the scheduler/orchestrator. Testing
the Spark job directly proves transformation correctness. Running it through
Airflow additionally proves scheduling, dependency ordering, retry visibility,
and operational history without putting data-processing code inside Airflow.

`./platform airflow-tasks` prints the two tasks. Their dependency is declared
in the DAG using `submit_spark_job() >> report_completion()`:

```text
submit_spark_job
    report_completion
```

The second task cannot run until the first succeeds. A failed Spark container
therefore makes the Airflow task and DAG run visibly fail instead of printing a
false completion message.
