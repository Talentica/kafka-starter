package com.talentica.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleKafkaConsumerManual {
	public static void main(String[] args) {

		String topic = "kafka-test-1";
		String groupId = "simple-java-consumer-groupid-2";

		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		
		props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		consumer.subscribe(Arrays.asList(topic));
		System.out.println("Subscribed to topic " + topic);
		while (true) {
			
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s, TimeStamp = %s%n", record.offset(), record.key(), record.value(), record.timestamp());
		}
	}


}
