/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.solr4_X.solrserver;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton pattern to instance CloudSolrServer
 * 
 * @author Chao Liu
 * @since SolrDemo 1.0
 */
public class CloudSolrServerFactory {

	private static CloudSolrServer solrServer = null;

	private static final String zkHost = "10.21.17.200:3183,10.21.17.201:3183";
	private static final int zkClientTimeout = 200000;
	private static final int zkConnectTimeout = 10000;

	private static Logger log = LoggerFactory
			.getLogger(CloudSolrServerFactory.class);

	private CloudSolrServerFactory() {
	}

	public static CloudSolrServer getSolrServerInstance(String collection) {
		if (solrServer == null) {
			synchronized (CloudSolrServerFactory.class) {
				if (solrServer == null) {
					try {
						solrServer = new CloudSolrServer(zkHost);
					} catch (Exception e) {
						e.printStackTrace();
						log.error(e.toString());
					}
					solrServer.setZkClientTimeout(zkClientTimeout);
					solrServer.setZkConnectTimeout(zkConnectTimeout);
					solrServer.setDefaultCollection(collection);
					solrServer.connect();
				}
			}
		}
		return solrServer;
	}
}
