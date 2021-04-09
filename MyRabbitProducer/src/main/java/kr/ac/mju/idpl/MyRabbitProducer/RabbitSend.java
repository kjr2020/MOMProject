package kr.ac.mju.idpl.MyRabbitProducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitSend {

	private static final String TASK_QUEUE_NAME = "sleep_queue";

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://rabbitmq:1234@117.17.158.60:5672/rHost");
		BufferedReader br = new BufferedReader(new FileReader("sleeptask.txt"));

		String line = br.readLine();
		long startTime = System.currentTimeMillis();
		try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
			channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

			while (line != null) {
				channel.basicPublish("", TASK_QUEUE_NAME, null, line.getBytes("UTF-8"));
				line = br.readLine();
			}
			PrintWriter pw = new PrintWriter("RabbitProducerResult.txt");
			pw.println("Rabbit Producer : " + (System.currentTimeMillis() - startTime));
			pw.close();

		}
		br.close();
	}

}