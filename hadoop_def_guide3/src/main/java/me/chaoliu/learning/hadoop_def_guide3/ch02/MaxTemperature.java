/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.hadoop_def_guide3.ch02;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Main class to call the mapreduce program
 * 
 * @author Chao Liu
 * @since Hadoop: The Definitive Duide 3
 */
public class MaxTemperature {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		/**
		 * if (args.length != 2) { System.err
		 * .println("Usage: MaxTemperature <input path> <output path>");
		 * System.exit(-1); }
		 */
		Job job = new Job();
		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max temperature");

		String inputPath = System.getProperty("user.dir")
				+ "/src/main/resources/sample.txt";
		String outPath = "output";

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
