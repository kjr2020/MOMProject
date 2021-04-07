import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;

public class RabbitSend {

	private static final String TASK_QUEUE_NAME = "sleep_queue";

	public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri("amqp://rabbitmq:1234@117.17.158.60:5672/rHost");
        BufferedReader br = new BufferedReader(new FileReader("sleeptask.txt"));
        //PrintWriter pw = new PrintWriter("StartTime.txt");
        String[] sleepTime = new String[1000];
        
       for (int i = 0 ; i < 1000 ; i++) {
        	String line = br.readLine();
        	sleepTime[i] = line;
        	if(line == null) break;
        	//i++;
        }
        try (Connection connection = factory.newConnection();
        	Channel channel = connection.createChannel()) {
            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

//            pw.println("Start Time : " + System.currentTimeMillis());
//            pw.close();
            for (int count = 0 ; count < sleepTime.length ; count++) {
            	channel.basicPublish("", TASK_QUEUE_NAME, null, sleepTime[count].getBytes("UTF-8"));
            }
        }
    }

}