/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.hadoop_def_guide3.ch04;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * TODO
 * 
 * @author Chao Liu
 * @since ??1.0
 */
public class StreamCompressor {

	public static void main(String[] args) throws ClassNotFoundException,
			IOException {

		String codecClassname = args[0];
		Class<?> codecClass = Class.forName(codecClassname);
		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec) ReflectionUtils
				.newInstance(codecClass, conf);

		CompressionOutputStream out = codec.createOutputStream(System.out);
		IOUtils.copyBytes(System.in, out, 4096, false);
		out.finish();
	}
}