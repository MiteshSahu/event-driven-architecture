FROM alpine:3.21 AS driver
RUN wget -q -O /postgresql.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar

FROM maven:3.9.9-eclipse-temurin-11 AS build
WORKDIR /workspace
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline
COPY src src
RUN mvn -q test package

FROM apache/spark:3.5.8-python3
USER root
COPY --from=driver /postgresql.jar /opt/spark/jars/postgresql.jar
COPY --from=build /workspace/target/daily-order-analytics.jar /opt/spark/jobs/daily-order-analytics.jar
ENTRYPOINT []
CMD ["/opt/spark/bin/spark-submit", "--class", "dev.learning.platform.analytics.DailyOrderAnalytics", "--jars", "/opt/spark/jars/postgresql.jar", "/opt/spark/jobs/daily-order-analytics.jar"]
