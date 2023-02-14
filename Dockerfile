FROM openjdk:8
ADD target/kafkaFlinkMongo-0.0.1-SNAPSHOT.jar kafka-flink-mongo
ENTRYPOINT ["java","-jar","kafka-flink-mongo"]