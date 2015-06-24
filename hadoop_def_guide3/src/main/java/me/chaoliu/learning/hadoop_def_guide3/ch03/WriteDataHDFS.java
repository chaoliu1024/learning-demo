/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.hadoop_def_guide3.ch03;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 * TODO
 * 
 * @author Chao Liu
 * @since ??1.0
 */
public class WriteDataHDFS {

	public static void main(String[] args) throws IOException {

		String dst = "hdfs://127.0.0.1/liuchao/sample.txt";
		Path path = new Path("hdfs://127.0.0.1/liuchao/sample.txt");
		Configuration conf = new Configuration();
		conf.setBoolean("dfs.support.append", true);
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		InputStream in = new BufferedInputStream(new ByteArrayInputStream(
				"contentrrrr".getBytes("UTF-8")));
		OutputStream out = fs.append(path);
		IOUtils.copyBytes(in, out, conf);
		out.close();
		fs.close();
		IOUtils.closeStream(in);
		in.close();
	}
}