/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.liuchao.learning.kafka.simple_consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.log.Log;
import kafka.message.MessageAndOffset;

/**
 * SimpleConsumer Example
 * 
 * The SimpleConsumer does require a significant amount of work not needed in
 * the Consumer Groups: <br/>
 * <ol>
 * <li>You must keep track of the offsets in your application to know where you
 * left off consuming.</li>
 * <li>You must figure out which Broker is the lead Broker for a topic and
 * partition.</li>
 * <li>You must handle Broker leader changes.</li>
 * </ol>
 * 
 * @author Chao Liu
 * @since Kafka Demo 1.0
 */
public class SimpleExample {

	private List<String> m_replicaBrokers = new ArrayList<String>();

	private static Logger log = LoggerFactory.getLogger(SimpleExample.class);

	public SimpleExample() {
		m_replicaBrokers = new ArrayList<String>();
	}

	/**
	 * The example expects the following parameters:
	 * <ol>
	 * <li>Maximum number of messages to read (so we don’t loop forever)</li>
	 * <li>Topic to read from</li>
	 * <li>Partition to read from</li>
	 * <li>One broker to use for Metadata lookup</li>
	 * <li>Port the brokers listen on</li>
	 * </ol>
	 * 
	 * @param args
	 */
	public static void main(String args[]) {

		SimpleExample example = new SimpleExample();

		// 3 lc 35116 10.21.17.200 9038
		// long maxReads = Long.parseLong(args[0]);
		// String topic = args[1];
		// int partition = Integer.parseInt(args[2]);
		// seeds.add(args[3]);
		// int port = Integer.parseInt(args[4]);
		long maxReads = Long.parseLong("1000");
		String topic = "lc";
		int partition = 0;
		List<String> seeds = new ArrayList<String>();
		seeds.add("10.21.17.200");
		seeds.add("10.21.17.201");
		seeds.add("10.21.17.202");
		int port = 9038;
		try {
			long start = System.currentTimeMillis();
			example.run(maxReads, topic, partition, seeds, port);
			long end = System.currentTimeMillis();
			log.info("running time: " + (end - start) + "ms");
		} catch (Exception e) {
			System.out.println("Oops:" + e);
			e.printStackTrace();
		}
	}

	public void run(long a_maxReads, String a_topic, int a_partition,
			List<String> a_seedBrokers, int a_port) throws Exception {

		// find the meta data about the topic and partition we are interested in
		PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic,
				a_partition);
		if (metadata == null) {
			System.out
					.println("Can't find metadata for Topic and Partition. Exiting");
			return;
		}
		if (metadata.leader() == null) {
			System.out
					.println("Can't find Leader for Topic and Partition. Exiting");
			return;
		}

		String leadBroker = metadata.leader().host();
		String clientName = "Client_" + a_topic + "_" + a_partition;

		SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port,
				100000, 64 * 1024, clientName);
		long readOffset = getLastOffset(consumer, a_topic, a_partition,
				kafka.api.OffsetRequest.EarliestTime(), clientName);
		int numErrors = 0;
		while (a_maxReads > 0) {
			if (consumer == null) {
				consumer = new SimpleConsumer(leadBroker, a_port, 100000,
						64 * 1024, clientName);
			}
			// When calling FetchRequestBuilder, it's important NOT to call
			// .replicaId(), which is meant for internal use only. Setting the
			// replicaId incorrectly will cause the brokers to behave
			// incorrectly.
			FetchRequest req = new FetchRequestBuilder().clientId(clientName)
					.addFetch(a_topic, a_partition, readOffset, 100000)
					// Note: this fetchSize of 100000 might need to be increased
					// if large batches are written to Kafka
					.build();
			FetchResponse fetchResponse = consumer.fetch(req);
			if (fetchResponse.hasError()) {
				numErrors++;
				// Something went wrong!
				short code = fetchResponse.errorCode(a_topic, a_partition);
				System.out.println("Error fetching data from the Broker:"
						+ leadBroker + " Reason: " + code);
				if (numErrors > 5)
					break;
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					// We asked for an invalid offset. For simple case ask for
					// the last element to reset
					readOffset = getLastOffset(consumer, a_topic, a_partition,
							kafka.api.OffsetRequest.LatestTime(), clientName);
					continue;
				}
				consumer.close();
				consumer = null;
				leadBroker = findNewLeader(leadBroker, a_topic, a_partition,
						a_port);
				continue;
			}
			numErrors = 0;
			long numRead = 0;
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(
					a_topic, a_partition)) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < readOffset) {
					System.out.println("Found an old offset: " + currentOffset
							+ " Expecting: " + readOffset);
					continue;
				}
				readOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				System.out.println(String.valueOf(messageAndOffset.offset())
						+ ": " + new String(bytes, "UTF-8"));
				numRead++;
				a_maxReads--;
			}
			if (numRead == 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {

				}
			}
		}
		if (consumer != null)
			consumer.close();
	}

	private String findNewLeader(String a_oldLeader, String a_topic,
			int a_partition, int a_port) throws Exception {

		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port,
					a_topic, a_partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host())
					&& i == 0) {
				// first time through if the leader hasn't changed give
				// ZooKeeper a second to recover second time, assume the
				// broker did recover before failover, or it was a non-Broker
				// issue
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		System.out
				.println("Unable to find new leader after Broker failure. Exiting");
		throw new Exception(
				"Unable to find new leader after Broker failure. Exiting");
	}

	/**
	 * Finding the Lead Broker for a Topic and Partition.
	 * 
	 * @param a_seedBrokers
	 * @param a_port
	 * @param a_topic
	 * @param a_partition
	 * @return
	 */
	private PartitionMetadata findLeader(List<String> a_seedBrokers,
			int a_port, String a_topic, int a_partition) {

		PartitionMetadata returnMetaData = null;
		loop: for (String seed : a_seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, a_partition, 100000,
						64 * 1024, "leaderLookup");
				List<String> topics = Collections.singletonList(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resq = consumer.send(req);

				List<TopicMetadata> metaData = resq.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				System.out.println("Error communicating with Broker [" + seed
						+ "] to find Leader for [" + a_topic + ", "
						+ a_partition + "] Reason: " + e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		if (returnMetaData != null) {
			m_replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				m_replicaBrokers.add(replica.host());
			}
		}
		return returnMetaData;
	}

	/**
	 * Finding Starting Offset for Reads.
	 * 
	 * @param consumer
	 * @param topic
	 * @param partition
	 * @param whichTime
	 * @param clientName
	 * @return
	 */
	public static long getLastOffset(SimpleConsumer consumer, String topic,
			int partition, long whichTime, String clientName) {

		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
				requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
				clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			System.out
					.println("Error fetching data Offset Data the Broker. Reason: "
							+ response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}
}
