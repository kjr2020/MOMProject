package kr.ac.mju.idpl.MyRabbitProducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitSend {
	
	static final String USER_NAME = "master";
	static final String PASSWORD = "1234";
	static final String VIRTUAL_HOST = "/";
	static final String HOST = "master";
	static final String WORKLOAD_FILE_NAME = "sleeptask";
	static final String QUEUE_NAME = "idpl-queue";
	static final String RESULT_FILE_NAME = "RabbitResult/RabbitProducer";
	static final int PORT = 5672;
	
	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUsername(USER_NAME);
		factory.setPassword(PASSWORD);
		factory.setVirtualHost(VIRTUAL_HOST);
		factory.setHost(HOST);
		factory.setPort(PORT);
		BufferedReader br = new BufferedReader(new FileReader(WORKLOAD_FILE_NAME));

		String line = br.readLine();
		long startTime = System.currentTimeMillis();
		try (Connection connection = factory.newConnection();
				Channel channel = connection.createChannel()) {
			channel.queueDeclare(QUEUE_NAME, true, false, false, null);

			while (line != null) {
				channel.basicPublish("", QUEUE_NAME, null, line.getBytes("UTF-8"));
				line = br.readLine();
			}
			PrintWriter pw = new PrintWriter(RESULT_FILE_NAME);
			pw.println("Rabbit Producer : " + (System.currentTimeMillis() - startTime));
			pw.close();

		}
		br.close();
	}

}