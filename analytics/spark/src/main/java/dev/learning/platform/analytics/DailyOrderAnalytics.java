package dev.learning.platform.analytics;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.storage.StorageLevel;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public final class DailyOrderAnalytics {

    private static final String JDBC_URL = "jdbc:postgresql://postgres:5432/orders";
    private static final List<String> ALLOWED_STATUSES =
            List.of("CREATED", "PROCESSING", "COMPLETED", "FAILED", "CANCELLED");

    private DailyOrderAnalytics() {
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("daily-order-analytics-java")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try {
            Properties jdbc = jdbcProperties();
            Dataset<Row> source = spark.read().jdbc(JDBC_URL, "processed_orders", jdbc);

            System.out.println("\n=== JAVA SPARK LAB 1: SOURCE ===");
            System.out.printf("Application=%s, master=%s, inputPartitions=%d%n",
                    spark.sparkContext().appName(),
                    spark.sparkContext().master(),
                    source.javaRDD().getNumPartitions());

            // Similar vocabulary to Stream.filter/map, but these Columns build a lazy,
            // distributed Spark plan rather than executing a local Java Stream pipeline.
            Dataset<Row> validOrders = cleanAndEnrich(source)
                    .persist(StorageLevel.MEMORY_AND_DISK());

            long validCount = validOrders.count(); // First Spark action materializes the cache.
            System.out.printf("Valid enriched orders cached=%d%n", validCount);
            validOrders.select("order_id", "normalized_status", "amount", "amount_band")
                    .show(false);

            Dataset<Row> metrics = aggregateAndRank(validOrders);

            System.out.println("\n=== JAVA SPARK LAB 2: PHYSICAL PLAN ===");
            System.out.println("Look for InMemoryTableScan, Exchange, HashAggregate, Window and Sort.");
            metrics.explain("formatted");

            System.out.println("\n=== JAVA SPARK LAB 3: ANALYTICAL RESULT ===");
            metrics.show(false);

            metrics.write()
                    .mode(SaveMode.Overwrite)
                    .jdbc(JDBC_URL, "daily_order_metrics", jdbc);

            validOrders.unpersist();
            System.out.println("Java Spark batch wrote daily_order_metrics successfully.\n");
        } finally {
            spark.stop();
        }
    }

    static Dataset<Row> cleanAndEnrich(Dataset<Row> source) {
        String allowedForLog = ALLOWED_STATUSES.stream()
                .map(String::toLowerCase)
                .sorted()
                .collect(Collectors.joining(", "));
        System.out.println("Allowed statuses prepared with a local Java Stream: " + allowedForLog);

        Column normalizedStatus = upper(trim(col("status")));

        return source
                .filter(col("order_id").isNotNull()
                        .and(col("customer_id").isNotNull())
                        .and(col("amount").isNotNull())
                        .and(col("amount").geq(0)))
                .withColumn("normalized_status", normalizedStatus)
                .filter(col("normalized_status").isin(ALLOWED_STATUSES.toArray()))
                .withColumn("metric_date", to_date(col("processed_at")))
                .withColumn("amount_band",
                        when(col("amount").leq(100), "LOW")
                                .when(col("amount").leq(500), "MEDIUM")
                                .otherwise("HIGH"));
    }

    static Dataset<Row> aggregateAndRank(Dataset<Row> validOrders) {
        Dataset<Row> aggregated = validOrders
                .groupBy("metric_date", "normalized_status", "amount_band")
                .agg(
                        count("order_id").alias("order_count"),
                        round(sum("amount"), 2).alias("total_amount"),
                        round(avg("amount"), 2).alias("average_amount"),
                        max("amount").alias("maximum_amount"),
                        countDistinct("customer_id").alias("unique_customers")
                );

        return aggregated
                .withColumn("revenue_rank",
                        dense_rank().over(Window
                                .partitionBy("metric_date")
                                .orderBy(col("total_amount").desc())))
                .withColumnRenamed("normalized_status", "status")
                .orderBy("metric_date", "revenue_rank", "status", "amount_band");
    }

    private static Properties jdbcProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", "platform");
        properties.setProperty("password", "platform");
        properties.setProperty("driver", "org.postgresql.Driver");
        return properties;
    }
}
