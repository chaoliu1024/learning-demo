/*
 * Copyright (c) 2015, Person Chao Liu. All rights reserved.
 */

package me.chaoliu.learning.solr4.X.index;

import java.util.Iterator;
import java.util.List;

import me.chaoliu.learning.solr4.X.solrserver.HttpSolrServerFactory;

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
		facet.facet();
	}

	String param1 = "q=*+&collection=collection1&sort=recordTime+desc";
	String param2 = "q=*+&facet.field=level&start=0&collection=collection1&sort=recordTime+desc&rows=10&facet=true";
	String param3 = "q=*+&facet.field=recordTime&start=0&collection=collection1&sort=recordTime+desc&rows=10&facet=true";
	String param5 = "q=*+&facet.date=recordTime&facet.date.gap=%2B1DAY&start=0&collection=collection1&sort=recordTime+desc&facet.date.end=NOW-0DAY&rows=10&facet=true&facet.date.start=NOW-371DAY%2FDAY";
	String param6 = "q=*+&facet.field=keywords&start=0&collection=collection1&sort=recordTime+desc&rows=10&facet=true";
	String param7 = "q=*+&facet.field=collectPro&start=0&collection=collection1&sort=recordTime+desc&rows=10&facet=true";

	public void facet() {

		int numCount = 0;
		while (numCount < 100000) {
			SolrQuery params = new SolrQuery("*:*");
			params.setFacet(true);
			if (numCount % 11 == 0) {
				params.set("facet.field", "popularity");
			} else if (numCount % 13 == 0) {
				params.set("facet.field", "keywords");
			} else if (numCount % 14 == 0) {
				params.set("facet.field", "url");
			} else if (numCount % 15 == 0) {
				params.set("facet.field", "description");
			}

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
						System.out.println(count.getName() + " : "
								+ count.getCount());
					}
					// System.out.println(facetField.getName() + " : "
					// + facetField.getValueCount());
				}
				SolrDocumentList results = response.getResults();
				for (int i = 0; i < results.size(); i++) {
					SolrDocument doc = results.get(i);
					Iterator<String> iterator = doc.keySet().iterator();
					while (iterator.hasNext()) {
						String key = iterator.next();
						logger.info(key + ":" + doc.getFieldValue(key));
					}
					System.out.println();
				}
			} catch (SolrServerException e) {
				e.printStackTrace();
			}
		}
		numCount++;
	}
}
