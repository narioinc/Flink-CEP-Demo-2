package com.narioinc.flinkdemos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.PatternProcessFunction.Context;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.narioinc.flinkdemos.datasource.FlinkKafkaSource;
import com.narioinc.flinkdemos.models.Device;
import com.narioinc.flinkdemos.models.Rule;
import com.narioinc.flinkdemos.queue.SQSQueueManager;

public class FlinkCEPWithSQS {
	private Rule rule;
	private Device device;
	private DataStream<ObjectNode> deviceTypeFilteredStream;

	public FlinkCEPWithSQS(Rule rule, Device device){
		this.rule = rule;
		this.device = device;
	}

	public void start() throws Exception{
		
		// create execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		
		FlinkKafkaSource source = FlinkKafkaSource.getSource();

		DataStream<ObjectNode> stream = env.addSource(source.getKafkaConsumer());
		final String deviceType = rule.getDeviceType();
		deviceTypeFilteredStream = stream.filter(new FilterFunction<ObjectNode>() {

			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(ObjectNode value) throws Exception {
				if (value.get("value").get("deviceType").asText().contentEquals(deviceType))
					return true;
				else
					return false;
				//return true;
			}
		});
		
		
		
		final OutputTag<ObjectNode> outputTag = new OutputTag<ObjectNode>(device.getDeviceID() + "-output") {
			private static final long serialVersionUID = 1L;
		};

		SingleOutputStreamOperator<ObjectNode> deviceFilteredStream = deviceTypeFilteredStream.process(new ProcessFunction<ObjectNode, ObjectNode>() {

			private static final long serialVersionUID = 1L;
			final String deviceID = device.getDeviceID();
			@Override
			public void processElement(ObjectNode value, ProcessFunction<ObjectNode, ObjectNode>.Context ctx,
					Collector<ObjectNode> out) throws Exception {
				// TODO Auto-generated method stub
				if(value.get("value").get("deviceID").asText().contentEquals(deviceID)) {
					ctx.output(outputTag, value);
				}
			}
		});

		DataStream<ObjectNode> device001Stream = deviceFilteredStream.getSideOutput(outputTag);
		//device001Stream.print();


		AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
		Pattern<ObjectNode, ?> pattern = Pattern.<ObjectNode>begin("start", skipStrategy).where(new SimpleCondition<ObjectNode>() {

			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(ObjectNode value) throws Exception {
				// TODO Auto-generated method stub
				return (Integer.parseInt(value.get("value").get("temp").asText()) < 21);
			}
		}).times(rule.getOccurence()).within(Time.milliseconds(rule.getDuration()));

		PatternStream<ObjectNode> patternStream = CEP.pattern(device001Stream, pattern);

		DataStream<String> result = patternStream.process(
				new PatternProcessFunction<ObjectNode, String>() {

					private static final long serialVersionUID = 1L;
					String deviceID = device.getDeviceID();
					@Override
					public void processMatch(Map<String, List<ObjectNode>> match, Context ctx, Collector<String> out) throws Exception {
						// TODO Auto-generated method stub
						System.out.println("here is the match for device id : " + deviceID);
						for (ObjectNode node : match.get("start")) {  
							System.out.print(node.get("value").get("temp") + " ");
						}
					}
				});
		
		env.execute();

	}

}
