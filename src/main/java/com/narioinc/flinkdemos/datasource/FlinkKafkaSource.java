package com.narioinc.flinkdemos.datasource;

import java.util.Properties;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

public class FlinkKafkaSource {
	
	static FlinkKafkaSource source;
	static FlinkKafkaConsumer<ObjectNode> consumer;
	
	private FlinkKafkaSource() { };
	
	public static FlinkKafkaSource getSource(){
		if(source == null) {
			source = new FlinkKafkaSource();
			Properties properties = new Properties();
			properties.setProperty("bootstrap.servers", "localhost:9092");
			properties.setProperty("group.id", "flink_consumer");
			
			consumer = new FlinkKafkaConsumer<ObjectNode>("source-topic", new JSONKeyValueDeserializationSchema(false), properties);
			consumer.setStartFromLatest();
		}
		
		return source;
	}
	
	public FlinkKafkaConsumer<ObjectNode> getKafkaConsumer() {
		return this.consumer;
	}
	
}
