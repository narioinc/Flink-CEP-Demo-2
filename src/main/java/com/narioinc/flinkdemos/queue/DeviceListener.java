package com.narioinc.flinkdemos.queue;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import com.amazonaws.services.alexaforbusiness.model.Device;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class DeviceListener implements MessageListener{
	
	Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	@Override
	public void onMessage(Message message) {
		// TODO Auto-generated method stub
		try {
			Device device = gson.fromJson(((TextMessage)message).getText(), Device.class);
			System.out.println(device.toString());
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

}
