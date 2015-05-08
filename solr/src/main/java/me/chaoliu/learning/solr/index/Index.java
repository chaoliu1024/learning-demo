/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.solr.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;

import me.chaoliu.learning.solr.solrserver.HttpSolrServerFactory;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

/**
 * operations of solr index
 * 
 * @author Chao Liu
 * @since SolrDemo 1.0
 */
public class Index {

	private Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	private SolrServer solrServer = HttpSolrServerFactory
			.getInstanceSolrServer();
	private static final Logger logger = Logger.getLogger(Index.class);

	public void addDocs(int totalDocNum, int batchNum) {
		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= batchNum; ++i) {

			addDoc(i);
			if (i % batchNum == 0) {
				long start = System.currentTimeMillis();
				commit();
				long end = System.currentTimeMillis();
				logger.info(i + ": " + (end - start) + "ms\t" + new Date());
			}
		}
		commit();
		long endTime = System.currentTimeMillis();
		logger.info("rate is " + totalDocNum / ((endTime - startTime) / 1000)
				+ " doc/s");
	}

	public void addDoc(int i) {

		SolrInputDocument doc = new SolrInputDocument();

		Random random = new Random();
		doc.addField("id", UUID.randomUUID());
		doc.addField("popularity", Math.abs(random.nextInt()) % 100 + 8000);
		doc.addField("last_modified", new java.util.Date());
		doc.addField("keywords", "2solr4.10");
		doc.addField("url", "127.0.0.5");
		// doc.addField("_route_", "shard1");
		docs.add(doc);
	}

	public void commit() {
		try {
			if (docs.size() == 0)
				return;
			solrServer.add(docs);
			solrServer.commit();
			docs.clear();
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public SolrDocumentList query(String field, String value) {

		String query = field + ":" + value;
		SolrQuery params = new SolrQuery(query);
		params.set("rows", 20);
		SolrDocumentList list = null;
		try {
			QueryResponse response = solrServer.query(params);
			list = response.getResults();
			for (int i = 0; i < list.size(); i++) {
				SolrDocument doc = list.get(i);
				Iterator<String> iterator = doc.keySet().iterator();
				while (iterator.hasNext()) {
					String key = iterator.next();
					logger.info(key + ":" + doc.getFieldValue(key));
				}
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return list;
	}

	public static void main(String[] args) {

		Index index = new Index();
		index.query("*", "*");
		// index.deleteAll();
		// index.addDocs(100000, 10000);
	}
}
