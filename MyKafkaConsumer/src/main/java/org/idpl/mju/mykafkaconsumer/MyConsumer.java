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
	static final String BOOTSTRAP_SERVER = "master:9092,slave1:9092,slave2:9092";
	static final String TOPIC_NAME = "idpl-topic";
	static final String GROUP_ID = "idpl-consumer";
	static final String ENABLE_AUTO_COMMIT = "true";
	static final String AUTO_OFFSET_RESET = "latest";
	static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	static final String RESULT_FILE_NAME = "/home/hjlee/DispatchingPerformance/Kafka/Consumer-";
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTRAP_SERVER);
		props.put("group.id", GROUP_ID);
		props.put("enable.auto.commit", ENABLE_AUTO_COMMIT);
		props.put("auto.offset.reset", AUTO_OFFSET_RESET);
		props.put("key.deserializer", KEY_DESERIALIZER);
		props.put("value.deserializer", VALUE_DESERIALIZER);
		props.put("fetch.min.bytes", 80);

		int partitionNum = Integer.parseInt(args[0]);

		long executeTime = System.currentTimeMillis();
		long executable = executeTime + 10000;

		// Create Kafka Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		// Connect partition to Kafka Consumer
		TopicPartition partition = new TopicPartition(TOPIC_NAME, partitionNum);
		consumer.assign(Arrays.asList(partition));
		consumer.seek(partition, 0);

		PrintWriter pw = new PrintWriter(RESULT_FILE_NAME + args[0]);

		pw.println("Consumer-" + args[0] + " Start Time : " + (System.currentTimeMillis() / 1000));

		try {
			while (executeTime < executable) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				// Parse String to Long and sleep Thread
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("Value : %s, Offset : %d\n", record.value(), record.offset());
					executable = System.currentTimeMillis() + 10000;
				}
				// Wait for not arrived task in 10sec
				executeTime = System.currentTimeMillis();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// Text file : File for check Consumer's running time.
			pw.println("Consumer-" + args[0] + " End Time : " + (System.currentTimeMillis() / 1000));
			pw.close();
			consumer.close();
			System.out.println("Consumer is Closed..");
		}
	}

}
