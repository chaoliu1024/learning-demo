/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.solr4_X.solrserver;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

/**
 * Singleton pattern to instance HttpSolrServer
 * 
 * @author Chao Liu
 * @since SolrDemo 1.0
 */
public class HttpSolrServerFactory {

	private static HttpSolrServer solrServer = null;
	private static final String URL = "http://127.0.0.1:8080/solr-4.10.0-web";

	// private static final String URL =
	// "http://127.0.0.1:8080/solr-5.0.0-web/collection1";

	// private static final String URL =
	// "http://10.21.17.200:9580/solr-4.10.0-web/perfTest201503";

	private HttpSolrServerFactory() {
	}

	public static SolrServer getInstanceSolrServer() {
		if (solrServer == null) {
			synchronized (HttpSolrServerFactory.class) {
				if (solrServer == null) {
					solrServer = new HttpSolrServer(URL);
					// defaults to 0. > 1 not recommended.
					solrServer.setMaxRetries(1);
					// 5 seconds to establish TCP
					solrServer.setConnectionTimeout(50000);
					// socket read timeout
					solrServer.setSoTimeout(100000);
					solrServer.setDefaultMaxConnectionsPerHost(100);
					solrServer.setMaxTotalConnections(1000);
					solrServer.setFollowRedirects(false);
					solrServer.setAllowCompression(true);
				}
			}
		}
		return solrServer;
	}
}
