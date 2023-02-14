package com.nest.bluehydrogen.kafkaFlinkMongo;

import com.nest.bluehydrogen.kafkaFlinkMongo.flink.FlinkConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class KafkaFlinkMongoApplication {
	@Autowired
	FlinkConsumer flinkConsumer;
	public static void main(String[] args){
		ApplicationContext context = SpringApplication.run(KafkaFlinkMongoApplication.class, args);
		KafkaFlinkMongoApplication kafkaFlinkMongoApplication = context.getBean(KafkaFlinkMongoApplication.class);
		kafkaFlinkMongoApplication.run();
	}
	public void run(){
		flinkConsumer.start();
	}
}
