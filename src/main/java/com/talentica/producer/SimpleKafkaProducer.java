package com.talentica.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleKafkaProducer 
{

	public static void main(String[] args) {
		
		Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092");
		 props.put("acks", "all");
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		 Producer<String, String> producer = new KafkaProducer<>(props);
		 for (int i = 0; i < 100; i++) {
			 String kafkaTopic = "kafka-test-1";
			 String key  = Integer.toString(i);
			 String value = Integer.toString(i);
		 	
		     producer.send(new ProducerRecord<String, String>(kafkaTopic, key, value));
		 }
		 producer.close();
		 

	}

}
