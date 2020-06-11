package com.talentica.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleKafkaProducerwithCompression {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		//Linger up to 100 ms before sending batch if size not met
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        //Batch up to 64K buffer sizes.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,  16_384 * 4);
        //Use Snappy compression for batch compression.
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 100; i < 300; i++) {
			String kafkaTopic = "kafka-test-compressed-1";
			String key = Integer.toString(i);
			String value = Integer.toString(i);

			producer.send(new ProducerRecord<String, String>(kafkaTopic, key, value));
		}
		producer.close();	

	}

}
