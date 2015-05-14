/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.liuchao.learning.kafka.high_level_consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * High level Consumer
 * 
 * @author Chao Liu
 * @since Kafka Demo 1.0
 */
public class ConsumerTest implements Runnable {

	private static Logger log = LoggerFactory.getLogger(ConsumerTest.class);
	private KafkaStream m_stream;
	private int m_threadNumber;

	public ConsumerTest(KafkaStream m_stream, int m_threadNumber) {
		this.m_stream = m_stream;
		this.m_threadNumber = m_threadNumber;
	}

	public void run() {

		long start = System.currentTimeMillis();
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		while (it.hasNext()) {
			System.out.println(Thread.currentThread().getName() + ": "
					+ new String(it.next().message()));
		}
		long end = System.currentTimeMillis();
		log.info(Thread.currentThread().getName() + "total time is "
				+ (end - start) + "ms");
		System.out.println("Shutting down Thread: " + m_threadNumber);
	}
}
