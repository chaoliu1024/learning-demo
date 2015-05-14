/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.liuchao.learning.kafka.high_level_consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * High Level Consumer
 * 
 * @author Chao Liu
 * @since Kafka Demo 1.0
 */
public class ConsumerGroupExample {

	private static Logger log = LoggerFactory
			.getLogger(ConsumerGroupExample.class);
	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;

	public ConsumerGroupExample(String a_zookeeper, String a_groupId,
			String a_topic) {
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig(a_zookeeper,
						a_groupId));
		this.topic = a_topic;
	}

	private static ConsumerConfig createConsumerConfig(String a_zookeeper,
			String a_groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", a_groupId);
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "2000");
		props.put("auto.commit.interval.ms", "1000000");

		return new ConsumerConfig(props);
	}

	public void shutdown() {
		if (consumer != null) {
			consumer.shutdown();
		}
		if (executor != null) {
			executor.shutdown();
		}
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out
						.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			System.err
					.println("Interrupted during shutdown, exiting uncleanly");
		}
	}

	public void run(int a_numThreads) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(a_numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		// now launch all the thread
		executor = Executors.newFixedThreadPool(a_numThreads);

		// now create an object to consume the messages
		int threadNumber = 0;
		for (final KafkaStream stream : streams) {
			executor.submit(new ConsumerTest(stream, threadNumber));
			threadNumber++;
		}
	}

	public static void main(String[] args) {
		// 10.21.17.200:3183,10.21.17.201:3183 lc-group test1 4
		String zooKeeper = "10.21.17.200:3183,10.21.17.201:3183";
		String groupId = "lc-group";
		String topic = "test2";
		int threads = 1;
		// String zooKeeper = args[0];
		// String groupId = args[1];
		// String topic = args[2];
		// int threads = Integer.parseInt(args[3]);

		long start = System.currentTimeMillis();
		ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper,
				groupId, topic);
		example.run(threads);

		try {
			Thread.sleep(500000);
		} catch (InterruptedException ie) {

		}
		example.shutdown();
		long end = System.currentTimeMillis();
		log.info((end - start) + " ms");
	}
}
