package com.narioinc.flinkdemos;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.narioinc.flinkdemos.models.Device;
import com.narioinc.flinkdemos.models.Rule;
import com.narioinc.flinkdemos.queue.SQSQueueManager;

public class FlinkAppManager implements MessageListener {

	
	private Gson gson = new GsonBuilder().setPrettyPrinting().create();
	private Rule rule;
	private SQSQueueManager manager;
	
	public FlinkAppManager(Rule rule){
		this.rule = rule;
		manager = new SQSQueueManager();
	}
	
	public void start() {
		try {
			manager.setMessageListenerForQueue("videojet-devices.fifo", this);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void onMessage(Message message) {
		try {
			Device device = gson.fromJson(((TextMessage)message).getText(), Device.class);
			System.out.println(device.toString());
			message.acknowledge();
			createDeviceStreamProcessor(rule, device);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void createDeviceStreamProcessor(Rule rule, Device device){
		FlinkCEPWithSQS processor = new FlinkCEPWithSQS(rule, device);
		try {
			processor.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
