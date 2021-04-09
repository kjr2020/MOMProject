package org.idpl.mju.mykafkaproducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyProducer {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//Properties for Kafka Producer
		Properties props = new Properties();
		props.put("bootstrap.servers", "master:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("batch.size", "4");
		
		//Text file : File for check Producer's running time.
		PrintWriter pw = new PrintWriter("ProducerResult.txt");
		String line = null;
		
		//Text file : This file has Integers that become sleep task
		BufferedReader br = new BufferedReader(new FileReader("sleeptask.txt"));
        
		//Producer's start time
		long producerStartTime = System.currentTimeMillis();
		
		//Create kafka Producer
		Producer<String, String> producer = new KafkaProducer<>(props);
		
		//Read sleeptask.txt and Producing to kafka queue
		line=br.readLine();
		while(line !=null){
			producer.send(new ProducerRecord<String, String>("idpl-topic", line));
			line=br.readLine();
		}
		
		//Write Producer End time in ProducerResult.txt file
		pw.println("Producer ExecutionTime : " + (System.currentTimeMillis() - producerStartTime));
		
		//close all files and kafkaProducer
		br.close();
		pw.close();
		producer.close();
	}

}
