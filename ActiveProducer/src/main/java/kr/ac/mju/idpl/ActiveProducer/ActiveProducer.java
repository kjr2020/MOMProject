package kr.ac.mju.idpl.ActiveProducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

public class ActiveProducer {

	static final String QUEUE_NAME = "idpl-queue";
	static final String CONNECTION_URI = "tcp://master:61616";
	static final String RESULT_FILE_NAME = "ActiveResult/ActiveProducer";
	static final String WORKLOAD_FILE_NAME = "sleeptask";
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(CONNECTION_URI);
		Connection connection = factory.createConnection();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		BufferedReader br = new BufferedReader(new FileReader(WORKLOAD_FILE_NAME));

		Queue queue = new ActiveMQQueue(QUEUE_NAME);

		MessageProducer producer = session.createProducer(queue);
		Destination destination = session.createQueue(QUEUE_NAME);

		String line = br.readLine();
		
		long startTime = System.currentTimeMillis();
		while (line != null) {
			Message message = session.createTextMessage(line);
			message.setJMSReplyTo(destination);
			producer.send(queue, message);
			System.out.println("[SEND]" + message.toString());
			line = br.readLine();
		}
		PrintWriter pw = new PrintWriter(RESULT_FILE_NAME);
		pw.println("Producer Execute Time : " + 
					(System.currentTimeMillis() - startTime));

		br.close();
		pw.close();
		session.close();
		connection.close();
	}

}
