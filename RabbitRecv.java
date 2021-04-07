import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.PrintWriter;

public class RabbitRecv {

    private static final String TASK_QUEUE_NAME = "sleep_queue";
    static int taskCount = 0;
    static long executeTime = 0;
    static long currentTime = 0;
    static long startTime = 0;
    //static PrintWriter pw = null;
    
    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri("amqp://mymac:1234@117.17.158.60:5672/rHost");
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        //pw = new PrintWriter("RecvFile.txt");

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        channel.basicQos(10);

        currentTime = System.currentTimeMillis();
        executeTime = currentTime +10000;
        
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");

            try {
            	doWork(message);
            	taskCount++;
            	executeTime = currentTime + 10000;
            } finally {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                System.out.println(taskCount + " Task Arrived.");
            }
        };
        
        while(currentTime < executeTime) {
        	channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> { });
        	currentTime = System.currentTimeMillis();
       }
    }

    private static void doWork(String task) {        
    	try {
    		Thread.sleep(Integer.parseInt(task));
    		//pw.println("End Time : " + System.currentTimeMillis());
        } catch (InterruptedException _ignored) {        
    		Thread.currentThread().interrupt();
    	}
    }
}
