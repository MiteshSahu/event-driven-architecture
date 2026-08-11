from datetime import datetime

from airflow.sdk import dag, task
import docker


NETWORK = "mini-data-platform_default"
SPARK_IMAGE = "mini-data-platform-spark-analytics"


@dag(
    dag_id="daily_order_analytics",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["learning", "spark", "batch"],
)
def daily_order_analytics():
    @task
    def submit_spark_job():
        client = docker.from_env()
        output = client.containers.run(
            image=SPARK_IMAGE,
            command=[
                "/opt/spark/bin/spark-submit",
                "--class", "dev.learning.platform.analytics.DailyOrderAnalytics",
                "--jars", "/opt/spark/jars/postgresql.jar",
                "/opt/spark/jobs/daily-order-analytics.jar",
            ],
            network=NETWORK,
            remove=True,
            stdout=True,
            stderr=True,
        )
        print(output.decode("utf-8"))

    @task
    def report_completion():
        print("Spark wrote daily_order_metrics; verify with ./platform analytics-results")

    submit_spark_job() >> report_completion()


daily_order_analytics()
