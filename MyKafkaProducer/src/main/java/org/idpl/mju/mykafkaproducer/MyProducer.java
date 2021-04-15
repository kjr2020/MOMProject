package org.idpl.mju.mykafkaproducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyProducer {

	static final String BOOTSTRAP_SERVER = "master:9092";
	static final String TOPIC_NAME = "idpl-topic";
	static final String WORKLOAD_FILE_NAME = "sleeptask";
	static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	static final String BATCH_SIZE = "4";
	static final String RESULT_FILE_NAME = "KafkaResult/KafkaProducer";
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//Properties for Kafka Producer
		Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTRAP_SERVER);
		props.put("key.serializer", KEY_SERIALIZER);
		props.put("value.serializer", VALUE_SERIALIZER);
		props.put("batch.size", BATCH_SIZE);
		
		//Text file : File for check Producer's running time.
		PrintWriter pw = new PrintWriter(RESULT_FILE_NAME);
		String line = null;
		
		//Text file : This file has Integers that become sleep task
		BufferedReader br = new BufferedReader(new FileReader(WORKLOAD_FILE_NAME));
        
		//Producer's start time
		long producerStartTime = System.currentTimeMillis();
		
		//Create kafka Producer
		Producer<String, String> producer = new KafkaProducer<>(props);
		
		//Read sleeptask.txt and Producing to kafka queue
		line=br.readLine();
		while(line !=null){
			producer.send(new ProducerRecord<String, String>(TOPIC_NAME, line));
			line=br.readLine();
		}
		
		//Write Producer End time in ProducerResult.txt file
		pw.println("Producer Execute Time : " + (System.currentTimeMillis() - producerStartTime));
		
		//close all files and kafkaProducer
		br.close();
		pw.close();
		producer.close();
	}

}
