package com.talentica.kafka.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

public class SimpleKafkaStreams {

	public static void main(String[] args) {
		Properties props = new Properties();
		 props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-processing-app-1");
		 props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		 props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		 props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		 StreamsBuilder builder = new StreamsBuilder();
		 //read from topic 1 and write to topic 2 by doing some aggragations 
		 builder.<String, String>stream("kafka-test-1").mapValues(value -> String.valueOf(value.length())).to("kafka-test-2");

		 KafkaStreams streams = new KafkaStreams(builder.build(), props);
		 streams.start();

	}

}
