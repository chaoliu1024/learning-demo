/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.liuchao.learning.kafka.producer;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Producer Demo
 * 
 * @author Chao Liu
 * @since Kafka Demo 1.0
 */
public class ProducerDemo {

	private static Logger log = LoggerFactory.getLogger(ProducerDemo.class);

	private static final String BROKER_LIST = "10.21.17.200:9038,10.21.17.201:9038,10.21.17.202:9038";
	private static String topic = "test2";

	public void sendEvent(long events) {

		ProducerConfig config = new ProducerConfig(producerProps());
		Producer<String, String> producer = new Producer<String, String>(config);
		List<KeyedMessage<String, String>> messagerList = new ArrayList<KeyedMessage<String, String>>();
		for (long nEvents = 1; nEvents <= events; nEvents++) {
			String msg = ",www.kafka.com,"
			// + ip
					+ "abcdefghijklmnop qrstuvwxyzabcdefghijklmnopqrstuvwxyza bcdefghijklmnopqrstuvwxyzabcdefghij klmnopqrstuvwxyzabcdefghijklmnopqrs  tuvwxyzabcdefghijklmn opqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh  ijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghi  jklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuv  wxyzabcdefghijklmnopqrstuvwxyzab  cdefghijklmnopqrstuvwxyzabcdefghijklmn  opqrstuvwxyzabcdefghijklmno  pqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl  mnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghi  jklmnopqrstuvwxyzabcdefghijklmnop  qrstuvwxyzabcdefghijkl";
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					topic, "212", msg);
			messagerList.add(data);
			if (nEvents % 50000 == 0) {
				producer.send(messagerList);
				messagerList.clear();
			}

		}
		producer.close();
	}

	private void batchSend(Producer<String, String> producer) {

	}

	private void send(Producer<String, String> producer) {
		Random rnd = new Random();
		// long runtime = new Date().getTime();
		// String ip = "192.168.2." + rnd.nextInt(255);
		// String msg = runtime + ",www.kafka.com," + ip;
		String msg = ",www.kafka.com,"
		// + ip
				+ "abcdefghijklmnop qrstuvwxyzabcdefghijklmnopqrstuvwxyza bcdefghijklmnopqrstuvwxyzabcdefghij klmnopqrstuvwxyzabcdefghijklmnopqrs  tuvwxyzabcdefghijklmn opqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh  ijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghi  jklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuv  wxyzabcdefghijklmnopqrstuvwxyzab  cdefghijklmnopqrstuvwxyzabcdefghijklmn  opqrstuvwxyzabcdefghijklmno  pqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl  mnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghi  jklmnopqrstuvwxyzabcdefghijklmnop  qrstuvwxyzabcdefghijkl";
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(
				topic, "212", msg);
		producer.send(data);
	}

	private Properties producerProps() {
		Properties props = new Properties();
		props.put("metadata.broker.list", BROKER_LIST);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class",
		// "me.liuchao.learning.kafka.producer.SimplePartitioner");
		props.put("request.required.acks", "0");

		props.put("compression.codec", "1");
		props.put("queue.enqueue.timeout.ms", "-1");
		props.put("producer.type", "async");
		props.put("batch.num.messages", "200");
		return props;
	}

	public static void main(String[] args) {
		// long events = Long.parseLong(args[0]);
		long events = 150000;
		ProducerDemo producer = new ProducerDemo();

		long start = System.currentTimeMillis();
		producer.sendEvent(events);
		long end = System.currentTimeMillis();
		log.info("rate = " + events / ((end - start) / 1000) + " event/s");
		log.info("the total time of sending " + events + " events is "
				+ (end - start) + "ms");
	}
}
