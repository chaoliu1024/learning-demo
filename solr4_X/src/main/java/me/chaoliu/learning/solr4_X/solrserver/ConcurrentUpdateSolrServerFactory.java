/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.solr4_X.solrserver;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton pattern to instance ConcurrentUpdateSolrServer
 * 
 * @author Chao Liu
 * @since SolrDemo 1.0
 */
public class ConcurrentUpdateSolrServerFactory {

	private static ConcurrentUpdateSolrServer solrServer = null;
	private static Logger log = LoggerFactory
			.getLogger(ConcurrentUpdateSolrServerFactory.class);

	private static final String URL = "http://127.0.0.1:8080/solr-4.10.0-web";

	private ConcurrentUpdateSolrServerFactory() {
	}

	/**
	 * @param queueSize
	 *            The buffer size before the documents are sent to the server
	 * @param threadCount
	 *            The number of background threads used to empty the queue
	 * @return solrServer
	 */
	public static SolrServer getSolrServerInstance(int queueSize,
			int threadCount) {
		if (solrServer == null) {
			synchronized (ConcurrentUpdateSolrServerFactory.class) {
				if (solrServer == null) {
					try {
						solrServer = new ConcurrentUpdateSolrServer(URL,
								queueSize, threadCount);
					} catch (Exception e) {
						e.printStackTrace();
						log.error(e.toString());

					}
					solrServer.setConnectionTimeout(60000);
					solrServer.setSoTimeout(60000);
				}
			}
		}
		return solrServer;
	}
}
