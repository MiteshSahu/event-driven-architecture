FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline
COPY src src
RUN mvn -q test package

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
RUN addgroup -S platform && adduser -S platform -G platform
COPY --from=build /workspace/target/csv-ingestion-0.1.0.jar app.jar
USER platform
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
