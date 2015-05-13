/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.lucene_in_action2.ch01;

import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ch01 demo -- Indexer
 * 
 * @author Chao Liu
 * @since Lucene In Action Demo 1.0
 */
public class Indexer {

	private IndexWriter writer;
	private static Logger logger = LoggerFactory.getLogger(Indexer.class);

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			throw new IllegalArgumentException("Usage: java "
					+ Indexer.class.getName() + " <index dir> <data dir>");
		}

		String indexDir = args[0];
		String dataDir = args[1];

		long start = System.currentTimeMillis();

		Indexer indexer = new Indexer(indexDir);
		int numIndexed;
		try {
			numIndexed = indexer.index(dataDir, new TextFilesFilter());
		} finally {
			indexer.close();
		}

		long end = System.currentTimeMillis();

		logger.info("Indexing " + numIndexed + " files took " + (end - start)
				+ " milliseconds");
	}

	public Indexer(String indexDir) throws IOException {
		Directory dir = FSDirectory.open(new File(indexDir));
		writer = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30),
				IndexWriter.MaxFieldLength.UNLIMITED);
	}

	public void close() throws CorruptIndexException, IOException {
		writer.close();
	}

	public int index(String dataDir, FileFilter filter) throws IOException {
		File[] files = new File(dataDir).listFiles();

		for (File f : files) {
			if (!f.isDirectory() && !f.isHidden() && f.exists() && f.canRead()
					&& (filter == null || filter.accept(f))) {
				indexFile(f);
			}
		}
		return writer.numDocs();
	}

	private void indexFile(File f) throws IOException {
		System.out.println("Indexing " + f.getCanonicalPath());
		Document doc = getDocument(f);
		writer.addDocument(doc);
	}

	protected Document getDocument(File f) throws IOException {
		Document doc = new Document();

		doc.add(new Field("contents", new FileReader(f)));
		doc.add(new Field("filename", f.getName(), Field.Store.YES,
				Field.Index.NOT_ANALYZED));
		doc.add(new Field("fullpath", f.getCanonicalPath(), Field.Store.YES,
				Field.Index.NOT_ANALYZED));
		return doc;
	}

	private static class TextFilesFilter implements FileFilter {

		public boolean accept(File path) {
			return path.getName().toLowerCase().endsWith(".txt");
		}
	}
}
