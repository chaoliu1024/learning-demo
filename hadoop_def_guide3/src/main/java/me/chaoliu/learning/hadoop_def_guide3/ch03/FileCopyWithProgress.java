/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.hadoop_def_guide3.ch03;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
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
public class FileCopyWithProgress {

	public static void main(String[] args) throws IOException {
		// String uri = args[0];
		String localSrc = "/home/liuchao/solrconfig--hdfs.xml";
		String dst = "hdfs://127.0.0.1/liuchao/indexinput2";
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);

		OutputStream out = fs.create(new Path(dst), new Progressable() {
			@Override
			public void progress() {
				System.out.println(".");
			}
		});
		IOUtils.copyBytes(in, out, 4096, false);
	}
}
