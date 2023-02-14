package com.nest.bluehydrogen.kafkaFlinkMongo.sink;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
@Getter@Setter
public class MongoDbSink extends RichSinkFunction<Document> {
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;
    private String uri;
    private String database;
    private String collectionName;

    public MongoDbSink(String uri, String database, String collectionName) {
        this.uri = uri;
        this.database = database;
        this.collectionName = collectionName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        mongoClient = MongoClients.create(uri);
        collection = mongoClient.getDatabase(database).getCollection(collectionName);
    }

    @Override
    public void close() throws Exception {
        mongoClient.close();
    }

    @Override
    public void invoke(Document value, Context context) throws Exception {
        collection.insertOne(value);
    }
}
