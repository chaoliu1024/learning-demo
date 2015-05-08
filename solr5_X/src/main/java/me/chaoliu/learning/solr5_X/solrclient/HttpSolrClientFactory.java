/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.solr5_X.solrclient;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

/**
 * Singleton pattern of HttpSolrClient Factory
 * 
 * @author Chao Liu
 * @since SolrDemo 1.0
 */
public class HttpSolrClientFactory {

	private static HttpSolrClient solrClient = null;

	private static final String URL = "http://127.0.0.1:8080/solr-5.0.0-web/collection1";

	private HttpSolrClientFactory() {
	}

	public static SolrClient getInstanceSolrClient() {
		if (solrClient == null) {
			synchronized (HttpSolrClientFactory.class) {
				if (solrClient == null) {
					solrClient = new HttpSolrClient(URL);
				}
			}
		}
		return solrClient;
	}
}