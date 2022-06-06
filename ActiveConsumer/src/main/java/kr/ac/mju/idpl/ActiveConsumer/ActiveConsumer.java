package kr.ac.mju.idpl.ActiveConsumer;

import java.io.PrintWriter;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;

public class ActiveConsumer {

	static final String QUEUE_NAME = "idpl-queue";
	static final String RESULT_FILE_NAME = "DispatchingPerformance/ActiveMQ/ActiveConsumer-";
	static final String CONNECTION_URI = "tcp://master:61616";

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(CONNECTION_URI);
		ActiveMQPrefetchPolicy fetch = new ActiveMQPrefetchPolicy();
		fetch.setQueuePrefetch(10);
		factory.setPrefetchPolicy(fetch);
		Connection connection = factory.createConnection();
		Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		int taskCount = 0;

		Destination destination = session.createQueue(QUEUE_NAME);
		MessageConsumer consumer = session.createConsumer(destination);

		connection.start();
		PrintWriter pw = new PrintWriter(RESULT_FILE_NAME + args[0]);
		pw.println("Consumer-" + args[0] + " Start Time : " + (System.currentTimeMillis()/1000));
		try {
			//sleep task
//			while (true) {
//				Message message = consumer.receive(10000);
//				System.out.println("Offset = " + message.getJMSMessageID() + 
//										"  Message = " + ((TextMessage) message).getText());
//				String tmp = ((TextMessage) message).getText();
//				Thread.sleep(Long.parseLong(tmp) * 1000);
//				taskCount++;
//				message.acknowledge();
//			}
			while (true) {
				Message message = consumer.receive(60000);
				System.out.println("Offset : " + taskCount + ", Message : "+((TextMessage) message).getText() + " Received..");
				taskCount++;
				message.acknowledge();
			}
		} finally {
			pw.println("Consumer-" + args[0] + " End Time : " + (System.currentTimeMillis()/1000));
			pw.println("Task Count : " + taskCount);

			pw.close();
			session.close();
			connection.close();

			System.out.println("Consumer Closed..");
		}
	}

}
