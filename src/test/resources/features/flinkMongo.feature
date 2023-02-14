Feature: Kafka Streams
  As an Architect, I should be able to use same template for different kafka-to-kafka streamlets.

  Scenario: Kafka is not running
    Given Kafka is not running

  Scenario: Check Kafka and Zookeeper are running
    Given Kafka and Zookeeper are installed and running
    Then Kafka should be up and running


  Scenario: Apache Flink is not running
    Given Apache Flink is not running

  Scenario: Apache Flink is running
    Given Apache Flink is installed and running
    Then Apache Flink should be up and running


  Scenario: MongoDB is not running
    Given MongoDB is not running

  Scenario: MongoDB is up and running
    Given MongoDB is installed and running
    Then I should successfully connect to the MongoDB server

#
  Scenario: Pass a message from Kafka to Apache Flink
    Given a message  "example message" to input-topic
    When the message is processed by a Kafka
    Then the message "example message" is passed to output-topic
    And the message "example message" is passed to Apache Flink


Scenario:  Successful message transfer from Apache Flink to MongoDB
  Given Apache Flink is  running
  Then the message is passed from Apache Flink to MongoDB











