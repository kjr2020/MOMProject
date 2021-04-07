package org.idpl.mju.mykafkaconsumer;

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

		long executeTime = System.currentTimeMillis();
		long executable = executeTime + 10000;
		long sleepTime = 0;
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		TopicPartition partition = new TopicPartition("test-topic2", 0);
		consumer.assign(Arrays.asList(partition));
		consumer.seek(partition, 0);
		
		//Thread.sleep(10000);
		
		try {
			while (executeTime < executable) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("Value : %s, Offset : %d\n", record.value(), record.offset());
					Thread.sleep(Long.parseLong(record.value()) * 5);
					executable = executeTime + 10000;
					System.out.println("executeTime is " + executeTime + " executable is " + executable);
				}
				executeTime = System.currentTimeMillis();
				try {
					consumer.commitSync();
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
			System.out.println("Consumer is Closed..");
		}
		consumer.close();
		System.out.println("Consumer is Closed..");
	}

}
