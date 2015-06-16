/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.solr4_X.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;

import me.chaoliu.learning.solr4_X.solrserver.HttpSolrServerFactory;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * operations of solr index
 * 
 * @author Chao Liu
 * @since SolrDemo 1.0
 */
public class Index {

	private Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

	private HttpSolrServer solrServer;

	private static Logger log = LoggerFactory.getLogger(Index.class);

	public Index() {

		// instantiate solrServer
		// solrServer =
		// CloudSolrServerFactory.getSolrServerInstance("ee201505");
		// solrServer = ConcurrentUpdateSolrServerFactory.getSolrServerInstance(
		// 50000, 4);
		solrServer = HttpSolrServerFactory.getSolrServerInstance();
	}

	public void addDocs(int totalDocNum, int batchNum) {
		double startTime = System.currentTimeMillis();
		for (int i = 1; i <= totalDocNum; ++i) {

			addDoc(i);
			if (i % batchNum == 0) {
				double start = System.currentTimeMillis();
				commit();
				double end = System.currentTimeMillis();
				log.info(i + ": " + batchNum / ((end - start) / 1000)
						+ "doc/s\t" + new Date());
			}
		}
		commit();
		double endTime = System.currentTimeMillis();
		log.info("rate is " + totalDocNum / ((endTime - startTime) / 1000)
				+ " doc/s");
		shutdown();
	}

	public void addDoc(int i) {

		SolrInputDocument doc = new SolrInputDocument();

		Random random = new Random();
		doc.addField("id", UUID.randomUUID());
		doc.addField("popularity", Math.abs(random.nextInt()) % 100 + 8000);
		doc.addField("last_modified", new java.util.Date());
		doc.addField("name", "10." + i % 23 + ".47." + i % 100);
		// doc.addField("_route_", "shard3");
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
		} finally {
			System.runFinalization();
			System.gc();
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
			System.out.println("size: " + list.size());
			for (int i = 0; i < list.size(); i++) {
				SolrDocument doc = list.get(i);
				Iterator<String> iterator = doc.keySet().iterator();
				while (iterator.hasNext()) {
					String key = iterator.next();
					log.info(key + ":" + doc.getFieldValue(key));
				}
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return list;
	}

	public void deleteAll() {
		try {
			solrServer.deleteByQuery("*:*");
			solrServer.commit();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}

	private void shutdown() {
		solrServer.shutdown();
	}

	public static void main(String[] args) {

		Index index = new Index();
		index.query("*", "*");
		// index.deleteAll();
		// index.addDocs(19032, 100000);
	}
}
