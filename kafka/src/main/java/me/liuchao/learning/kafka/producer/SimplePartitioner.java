/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.liuchao.learning.kafka.producer;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * TODO
 * 
 * @author Chao Liu
 * @since ??1.0
 */
public class SimplePartitioner implements Partitioner {

	public SimplePartitioner(VerifiableProperties props) {
	}

	public int partition(Object key, int a_numPartitions) {
		int partition = 0;
		String stringKey = (String) key;
		int offset = stringKey.lastIndexOf('.');
		if (offset > 0) {
			partition = Integer.parseInt(stringKey.substring(offset + 1))
					% a_numPartitions;
		}
		return partition;
	}
}
