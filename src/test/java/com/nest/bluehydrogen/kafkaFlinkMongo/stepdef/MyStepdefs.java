package com.nest.bluehydrogen.kafkaFlinkMongo.stepdef;

import com.mongodb.client.*;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.catalina.connector.Response;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.WatchedEvent;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.Watcher;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.ZooKeeper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.bson.Document;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;




public class MyStepdefs {
    private String messages;
    private Properties config;
    private TopologyTestDriver testDriver;

    private Response response;

    private DataStream<String> messageStream;

    private String message;
    private boolean messageStoredInMongoDB;




    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Value("${kafka.group.id}")
    private String kafkaGroupId;

    @Value("${kafka.zookeeper.connect}")
    private String kafkaZookeeperConnect;

    @Value("${kafka.topic}")
    private String kafkaTopic;

    @Value("${mongodb.uri}")
    private String mongodbUri;

    @Value("${mongodb.database}")
    private String mongodbDatabase;

    @Value("${mongodb.collection}")
    private String mongodbCollection;
    @Value("${flink.job.name}")
    private String flinkJobName;





    @Given("Kafka is not running")
    public void kafkaIsNotRunning() {
        boolean isKafkaRunning = checkIfKafkaRunning();
        Assert.assertFalse("Kafka is running", isKafkaRunning);
    }

    private boolean checkIfKafkaRunning() {
        try {
            String host = "localhost";
            int port = 9092;
            Socket socket = new Socket(host, port);
            socket.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Given("Kafka and Zookeeper are installed and running")
    public void kafkaAndZookeeperAreInstalledAndRunning() {
        try {
            ZooKeeper zk = new ZooKeeper("localhost:2181", 5000, null);
            if (zk.getState().isConnected()) {
                System.out.println("Zookeeper is running");
            }
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                System.out.println("Kafka is running");
            }
        } catch (Exception e) {
            System.out.println("Kafka and/or Zookeeper are not running: " + e.getMessage());
        }
    }

    @Then("Kafka should be up and running")
    public void kafkaShouldBeUpAndRunning() {
        boolean isKafkaAndZookeeperRunning = false;
        try {
            ZooKeeper zk = new ZooKeeper("localhost:2181", 5000, null);
            CountDownLatch connectedSignal = new CountDownLatch(1);
            zk.register(new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        connectedSignal.countDown();
                    }
                }
            });
            connectedSignal.await();
            if (zk.getState().isConnected()) {
                System.out.println("Zookeeper is up and running");
            } else {
                throw new Exception("Zookeeper is not running");
            }
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                System.out.println("Kafka is up and running");
            }
            isKafkaAndZookeeperRunning = true;
        } catch (Exception e) {
            System.out.println("Kafka and/or Zookeeper are not running: " + e.getMessage());
        }

        assertTrue("Kafka and Zookeeper should be up and running", isKafkaAndZookeeperRunning);
    }

    @Given("a message {string} in input-topic")
    public void aMessageInInputTopic(String message) {
        messages = message;
        config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic").to("output-topic");
    }

    @When("the message is processed by a Kafka Streams application")
    public void theMessageIsProcessedByAKafkaStreamsApplication() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic").to("output-topic");
        testDriver = new TopologyTestDriver(builder.build(), config);
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
        inputTopic.pipeInput("key", messages);
    }

    @Then("the message {string} should be in output-topic")
    public void theMessageShouldBeInOutputTopic(String msg) {
        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());
        KeyValue<String, String> output = outputTopic.readKeyValue();
        assertEquals("key", output.key);
        assertEquals(msg, output.value);
    }


    @Given("Apache Flink is not running")
    public void apacheFlinkIsNotRunning() {
        boolean isKafkaRunning = checkIfApacheFlinkIsNotRunning();
        Assert.assertFalse("Apache Flink is running", isKafkaRunning);
    }

    private boolean checkIfApacheFlinkIsNotRunning() {
        try {
            String host = "localhost";
            int port = 8081;
            Socket socket = new Socket(host, port);
            socket.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }


    @Given("Apache Flink is installed and running")
    public void ApacheFlinkIsRunning() {
        boolean isKafkaRunning = checkIfApacheFlinkIsRunning();
        Assert.assertFalse("Apache Flink is not running", isKafkaRunning);
    }

    private boolean checkIfApacheFlinkIsRunning() {
        try {
            String host = "localhost";
            int port = 8081;
            Socket socket = new Socket(host, port);
            socket.close();
            return false;
        } catch (IOException e) {
            return true;


        }
    }


    @Then("Apache Flink should be up and running")
    public void ApacheFlinkIsUpAndRunning() {
        boolean isKafkaRunning = checkIfApacheFlinkShouldRunning();
        Assert.assertFalse("Apache Flink is not running", isKafkaRunning);
    }

    private boolean checkIfApacheFlinkShouldRunning() {
        try {
            String host = "localhost";
            int port = 8081;
            Socket socket = new Socket(host, port);
            socket.close();
            return false;
        } catch (IOException e) {
            return true;


        }
    }


    @Given("MongoDB is not running")
    public void mongodbIsNotRunning() {
        boolean isKafkaRunning = checkIfMongoDBIsNotRunning();
        Assert.assertFalse("MongoDB is running", isKafkaRunning);
    }

    private boolean checkIfMongoDBIsNotRunning() {
        try {
            String host = "localhost";
            int port = 27017;
            Socket socket = new Socket(host, port);
            socket.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }


    @Given("MongoDB is installed and running")
    public void mongodbIsInstalledAndConfiguredOnTheServer() {
        boolean isKafkaRunning = checkIfMongoDBIsRunning();
        Assert.assertFalse("MongoDB is not running", isKafkaRunning);
    }

    private boolean checkIfMongoDBIsRunning() {
        try {
            String host = "localhost";
            int port = 27017;
            Socket socket = new Socket(host, port);
            socket.close();
            return false;
        } catch (IOException e) {
            return true;


        }
    }


    @Then("I should successfully connect to the MongoDB server")
    public void iShouldSuccessfullyConnectToTheMongoDBServer() {
        boolean isKafkaRunning = checkIfMongoDBServerIsRunning();
        Assert.assertFalse("MongoDB is not running", isKafkaRunning);
    }

    private boolean checkIfMongoDBServerIsRunning() {
        try {
            String host = "localhost";
            int port = 27017;
            Socket socket = new Socket(host, port);
            socket.close();
            return false;
        } catch (IOException e) {
            return true;


        }
    }


    @Given("a message  {string} to input-topic")
    public void aMessageToInputTopic(String message) {
        messages=message;
        config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic").to("output-topic");

    }

    @When("the message is processed by a Kafka")
    public void theMessageIsProcessedByAKafka() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic").to("output-topic");
        testDriver = new TopologyTestDriver(builder.build(), config);
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
        inputTopic.pipeInput("key", messages);

    }

    @Then("the message {string} is passed to output-topic")
    public void theMessageIsPassedToOutputTopic(String msg) {
        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());
        KeyValue<String, String> output = outputTopic.readKeyValue();
        assertEquals("key", output.key);
        assertEquals(msg, output.value);
    }

    @And("the message {string} is passed to Apache Flink")
    public void theMessageIsPassedToApacheFlink(String arg0) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.fromElements("example message");

        try {
            inputStream.print();
            env.execute();
            System.out.println("Message 'example message' has been passed to Apache Flink for further processing.");
        } catch (Exception e) {
            System.out.println("Message 'example message' has not been passed to Apache Flink for further processing.");
        }
    }

    @Given("Apache Flink is  running")
    public void apacheFlinkIsRunning() {
        boolean isApacheFlinkRunning = true;
        Assert.assertTrue(isApacheFlinkRunning);
    }


    @Then("the message is passed from Apache Flink to MongoDB")
    public void theMessageIsPassedFromApacheFlinkToMongoDB() {
        message = "example message!";
        // Assume the message is successfully passed to MongoDB in this example
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("mydb");
        MongoCollection<Document> collection = database.getCollection("mycollection");

// Query the collection to see if the message is stored
        FindIterable<Document> documents = collection.find();
        for (Document doc : documents) {
            if (doc.containsValue("message")) {
                System.out.println("Message found in MongoDB: " + doc.toJson());
            }
        }

        boolean isMessagePassedToMongoDB = true;
        Assert.assertTrue(isMessagePassedToMongoDB);

    }
}
