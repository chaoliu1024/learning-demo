/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.solr4_X.index;

import java.util.Iterator;
import java.util.List;

import me.chaoliu.learning.solr4_X.solrserver.HttpSolrServerFactory;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

/**
 * solr facet test
 * 
 * @author Chao Liu
 * @since SolrDemo 1.0
 */
public class Facet {

	public SolrServer server;
	private static final Logger logger = Logger.getLogger(Facet.class);

	public Facet() {
		this.server = HttpSolrServerFactory.getInstanceSolrServer();
	}

	public static void main(String[] args) {
		Facet facet = new Facet();
		facet.getfacetResult();
	}

	public void getfacetResult() {

		int loopCount = 0;
		while (loopCount < 100000) {
			SolrQuery params = new SolrQuery("*:*");

			params.setFacet(true);
			params.set("facet.field", "popularity");
			params.set("collection", "collection1");
			params.setSort("last_modified", SolrQuery.ORDER.desc);

			try {
				QueryResponse response = server.query(params);
				List<FacetField> facetFields = response.getFacetFields();
				for (int i = 0; i < facetFields.size(); i++) {
					FacetField facetField = facetFields.get(i);
					List<Count> values = facetField.getValues();
					for (int j = 0; j < values.size(); j++) {
						Count count = values.get(j);
						logger.info(count.getName() + " : " + count.getCount());
					}
				}
				SolrDocumentList results = response.getResults();
				for (int i = 0; i < results.size(); i++) {
					SolrDocument doc = results.get(i);
					Iterator<String> iterator = doc.keySet().iterator();
					while (iterator.hasNext()) {
						String key = iterator.next();
						logger.info(key + ":" + doc.getFieldValue(key));
					}
				}
			} catch (SolrServerException e) {
				e.printStackTrace();
			}
		}
		loopCount++;
	}
}
