package kr.ac.mju.idpl.MyRabbitProducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitSend {

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUsername("master");
		factory.setPassword("1234");
		factory.setVirtualHost("/");
		factory.setHost("master");
		factory.setPort(5672);
		BufferedReader br = new BufferedReader(new FileReader("sleeptask.txt"));

		String line = br.readLine();
		long startTime = System.currentTimeMillis();
		try (Connection connection = factory.newConnection();
				Channel channel = connection.createChannel()) {
			channel.queueDeclare("idpl-queue", true, false, false, null);

			while (line != null) {
				channel.basicPublish("", "idpl-queue", null, line.getBytes("UTF-8"));
				line = br.readLine();
			}
			PrintWriter pw = new PrintWriter("RabbitProducerResult.txt");
			pw.println("Rabbit Producer : " + (System.currentTimeMillis() - startTime));
			pw.close();

		}
		br.close();
	}

}