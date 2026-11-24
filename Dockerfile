FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/mini-mq-0.1.0.jar app.jar
VOLUME /data
EXPOSE 9092 8080
ENV MQ_DATA_DIR=/data
# -Xmx small on purpose: records live in the OS page cache, not the heap
ENTRYPOINT ["java","-Xms512m","-Xmx1g","-XX:+UseZGC","-jar","/app/app.jar"]
