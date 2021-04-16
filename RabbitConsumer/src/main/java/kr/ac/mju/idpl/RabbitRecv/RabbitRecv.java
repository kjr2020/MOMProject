package kr.ac.mju.idpl.RabbitRecv;

import java.io.IOException;
import java.io.PrintWriter;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RabbitRecv {
	
	static final String USER_NAME = "master";
	static final String PASSWORD = "1234";
	static final String VIRTUAL_HOST = "/";
	static final String HOST = "master";
	static final String WORKLOAD_FILE_NAME = "sleeptask";
	static final String QUEUE_NAME = "idpl-queue";
	static final String RESULT_FILE_NAME = "RabbitResult/RabbitConsumer-";
	static final int PORT = 5672;
	
	public static void main(String[] argv) throws Exception {
		PrintWriter pw = new PrintWriter(RESULT_FILE_NAME + argv[0]);

		ConnectionFactory factory = new ConnectionFactory();
		factory.setUsername(USER_NAME);
		factory.setPassword(PASSWORD);
		factory.setVirtualHost(VIRTUAL_HOST);
		factory.setHost(HOST);
		factory.setPort(PORT);

		
		// long startTime = System.currentTimeMillis();
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		channel.queueDeclare(QUEUE_NAME, true, false, false, null);

		channel.basicQos(1);

		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

		pw.println("Consumer-" + argv[0] + " Start Time : " + (System.currentTimeMillis()/1000));
		// channel.basicConsume("idpl-queue", true, deliverCallback, consumerTag -> {
		// });
		channel.basicConsume(QUEUE_NAME, false, "consumerTag", new DefaultConsumer(channel) {
			int taskCount = 0;
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] Body) throws IOException {
				long deliveryTag = envelope.getDeliveryTag();
				
				String temp = new String(Body, "UTF-8");
				System.out.println(temp + " Received..");

				try {
					Thread.sleep(Long.parseLong(temp)*1000);
					taskCount++;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				channel.basicAck(deliveryTag, false);
				
				if (channel.messageCount(QUEUE_NAME) == 0) {
						pw.println("Consumer-" + argv[0] + " End Time : " + (System.currentTimeMillis()/1000));
						pw.println("Task Count : " + taskCount);
						pw.close();
						connection.close();
						System.out.println("Connection Closed...");
				}else {
				}
				
				
			}
		});
	}

}