package kr.ac.mju.idpl.RabbitRecv;

import java.io.IOException;
import java.io.PrintWriter;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RabbitRecv implements AutoCloseable {

	static final String USER_NAME = "master";
	static final String PASSWORD = "1234";
	static final String VIRTUAL_HOST = "/";
	static final String HOST = "master";
	static final String QUEUE_NAME = "idpl-queue";
	static final String RESULT_FILE_NAME = "DispatchingPerformance/RabbitMQ/RabbitConsumer-";
	static final int PORT = 5672;
	static final int QOS = 10;
	static int taskCount = 0;
	private static Connection connection;
	private static Channel channel;
	private static PrintWriter pw;
//	private static String endTag;
//	private static String checkPoint;
	private static long endTime;
	private static long checkPoint;

	public static void main(String[] argv) throws Exception {
		pw = new PrintWriter(RESULT_FILE_NAME + argv[0]);
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUsername(USER_NAME);
		factory.setPassword(PASSWORD);
		factory.setVirtualHost(VIRTUAL_HOST);
		factory.setHost(HOST);
		factory.setPort(PORT);
//		endTag = "EndTag";
//		checkPoint = "ConsumerTag";
		int consumerTagInt = 0;

		RabbitRecv rabbitRecv = new RabbitRecv();

		// long startTime = System.currentTimeMillis();
		connection = factory.newConnection();
		channel = connection.createChannel();
		// RpcServer rpcServer = new RpcServer(channel, QUEUE_NAME);
		channel.basicQos(10);

		// Thread.sleep((10 - Integer.parseInt(argv[0]))*1000);
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		checkPoint = System.currentTimeMillis();
		endTime = checkPoint + 60000;
		pw.println("Consumer-" + argv[0] + " Start Time : " + (System.currentTimeMillis() / 1000));
//		while (!endTag.equals(checkPoint)) {
//			rabbitRecv.call(checkPoint);
//			System.out.println("CheckPoint : " + checkPoint +"\tEndTag : " + endTag);
//		}

		while(endTime > checkPoint) {
			rabbitRecv.call(Integer.toString(consumerTagInt));
			consumerTagInt++;
		}
		
		connection.close();
		System.out.println("Connection Closed...");
		pw.println("Consumer-" + argv[0] + " End Time : " + (System.currentTimeMillis() / 1000));
		pw.println("Task Count : " + taskCount);
		pw.close();
	}

	public void call(String cTag) throws Exception {
		String consumerTag = channel.basicConsume(QUEUE_NAME, false, cTag, new DefaultConsumer(channel) {

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] Body) throws IOException {
				long deliveryTag = envelope.getDeliveryTag();
				
				String temp = new String(Body, "UTF-8");
				taskCount++;
				System.out.println("Offset : " + taskCount + " arrived..");
				channel.basicAck(deliveryTag, true);
				endTime = System.currentTimeMillis() + 60000;
				// connection.close();
			}

		});
//		checkPoint = Integer.toString(taskCount);
//		endTag = consumerTag;
		checkPoint = System.currentTimeMillis();
	}

	@Override
	public void close() throws Exception {
		connection.close();
		// TODO Auto-generated method stub

	}

}