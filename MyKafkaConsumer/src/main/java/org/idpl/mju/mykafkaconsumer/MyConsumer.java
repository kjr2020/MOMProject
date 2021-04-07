package org.idpl.mju.mykafkaconsumer;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class MyConsumer {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Properties props = new Properties();
		props.put("bootstrap.servers", "master:9092");
		props.put("group.id", "test-consumer");
		props.put("enable.auto.commit", "false");
		props.put("auto.offset.reset", "latest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		//Text file : File for check Consumer's running time.
		PrintWriter pw = new PrintWriter("ConsumerResult" + args[0] + ".txt");
		
		int partitionNum = Integer.parseInt(args[0]);
		
		long executeTime = System.currentTimeMillis();
		long executable = executeTime + 10000;

		//Create Kafka Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		//Connect partition to Kafka Consumer
		TopicPartition partition = new TopicPartition("test-topic2", partitionNum);
		consumer.assign(Arrays.asList(partition));
		consumer.seek(partition, 0);
		
		long consumerStartTime = System.currentTimeMillis();
		try {
			while (executeTime < executable) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				
				//Parse String to Long and sleep Thread
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("Value : %s, Offset : %d\n", record.value(), record.offset());
					Thread.sleep(Long.parseLong(record.value()));
					executable = System.currentTimeMillis() + 10000;
					System.out.println("executeTime is " + executeTime + " executable is " + executable);
				}
				//Wait for not arrived task in 10sec
				executeTime = System.currentTimeMillis();
				try {
					
					//Commit consumer's offset
					consumer.commitSync();
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			pw.println("Consumer" + args[0] + " Execution Time : " + 
						(System.currentTimeMillis() - consumerStartTime));
			pw.close();
			consumer.close();
			System.out.println("Consumer is Closed..");
		}
//		consumer.close();
//		System.out.println("Consumer is Closed..");
	}

}
