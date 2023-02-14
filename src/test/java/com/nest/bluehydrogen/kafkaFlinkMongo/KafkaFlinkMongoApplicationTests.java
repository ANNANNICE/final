package com.nest.bluehydrogen.kafkaFlinkMongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.nest.bluehydrogen.kafkaFlinkMongo.flink.FlinkConsumer;
import com.nest.bluehydrogen.kafkaFlinkMongo.sink.MongoDbSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.boot.test.context.SpringBootTest;




@RunWith(MockitoJUnitRunner.class)
@SpringBootTest
public class KafkaFlinkMongoApplicationTests {



	private MongoClient mongoClient;
	private MongoCollection<Document> collection;
	private String uri = "mongodb://localhost:27017";
	private String database = "patient-db";
	private String collectionName = "patients";

	@Before
	public void setUp() throws Exception {
		mongoClient = MongoClients.create(uri);
		collection = mongoClient.getDatabase(database).getCollection(collectionName);
	}

	@After
	public void tearDown() throws Exception {
		collection.deleteMany(new Document());

		mongoClient.close();
	}

	@org.junit.Test
	public void testInvoke() throws Exception {
		MongoDbSink sink = new MongoDbSink(uri, database, collectionName);
		sink.open(null);
		Document document = new Document("name", "John");
		sink.invoke(document, null);
		Assert.assertEquals(1, collection.countDocuments());
		sink.close();
	}
	@Test
	void testStart() throws Exception {
		String kafkaBootstrapServers = "localhost:9092";
		String kafkaGroupId = "test-group";
		String kafkaZookeeperConnect = "localhost:2181";
		String kafkaTopic = "new";
		String mongodbUri = "mongodb://localhost:27017";
		String mongodbDatabase = "patient-db";
		String mongodbCollection = "patients";
		String flinkJobName = "test-job";


		StreamExecutionEnvironment env = Mockito.mock(StreamExecutionEnvironment.class);
		FlinkKafkaConsumer<String> consumer = Mockito.mock(FlinkKafkaConsumer.class);
		DataStream<String> dataStream = Mockito.mock(DataStream.class);
		DataStream<Document> transformedDataStream = Mockito.mock(DataStream.class);



		FlinkConsumer myClass = new FlinkConsumer();
		myClass.setKafkaBootstrapServers(kafkaBootstrapServers);
		myClass.setKafkaGroupId(kafkaGroupId);
		myClass.setKafkaZookeeperConnect(kafkaZookeeperConnect);
		myClass.setKafkaTopic(kafkaTopic);
		myClass.setMongodbUri(mongodbUri);
		myClass.setMongodbDatabase(mongodbDatabase);
		myClass.setMongodbCollection(mongodbCollection);
		myClass.setFlinkJobName(flinkJobName);

	}
}












