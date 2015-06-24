/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.hadoop_def_guide3.ch04;

import java.io.IOException;

import me.chaoliu.learning.hadoop_def_guide3.ch02.MaxTemperature;
import me.chaoliu.learning.hadoop_def_guide3.ch02.MaxTemperatureMapper;
import me.chaoliu.learning.hadoop_def_guide3.ch02.MaxTemperatureReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * TODO
 * 
 * @author Chao Liu
 * @since ??1.0
 */
public class MaxTemperatureWithCompression {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		// 新API
//		Configuration conf = new Configuration();
//		conf.setBoolean("mapred.compress.map.output", true);
//		conf.setClass("mapred.map.output.compression.codec", GzipCodec.class,
//				CompressionCodec.class);
		Job job = new Job();

		job.setJarByClass(MaxTemperature.class);
		FileInputFormat.addInputPath(job,
				new Path(System.getProperty("user.dir")
						+ "/src/main/resources/sample.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 旧API
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		job.setMapperClass(MaxTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}