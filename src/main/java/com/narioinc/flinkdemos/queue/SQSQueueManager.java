package com.narioinc.flinkdemos.queue;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;

public class SQSQueueManager {
	private ProfileCredentialsProvider credentialsProvider;
	private SQSConnection connection;
	private AmazonSQSMessagingClientWrapper client;
	private Session session;
	
	public SQSQueueManager() {
		credentialsProvider = new ProfileCredentialsProvider();
		try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
		
		SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
		        new ProviderConfiguration(),
		        AmazonSQSClientBuilder.standard().withRegion("us-east-1").withCredentials(credentialsProvider)
		        );
		 
		// Create the connection.
		try {
			connection = connectionFactory.createConnection();
			client = connection.getWrappedAmazonSQSClient();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	void initQueue(String queueUrl) throws JMSException{
		if (!client.queueExists(queueUrl)) {
		    client.createQueue(queueUrl);
		}
	}
	
	public void setMessageListenerForQueue(String queueName, MessageListener listener) throws JMSException{
		Queue queue = session.createQueue(queueName);
		MessageConsumer consumer = session.createConsumer(queue);
		consumer.setMessageListener(listener);
		connection.start();
	}
	
	void deleteMessage(Message m){
		try {
			client.deleteMessage(new DeleteMessageRequest().withReceiptHandle(m.getReceiptHandle()));
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
