package gr.iti.mklab.yfcc.dao;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import gr.iti.mklab.yfcc.models.Item;
import gr.iti.mklab.yfcc.models.TFIDF;
import gr.iti.mklab.yfcc.models.TFIDFVector;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;

public class SolrItemClient {
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	
	private HttpSolrClient server;
	
	public SolrItemClient(String solrUrl) {
		server = new HttpSolrClient(solrUrl);
	}
	
	public void index(Item item) throws IOException, SolrServerException {
		server.addBean(item);
		server.commit();
	}
	
	public int index(List<Item> items) throws IOException, SolrServerException {
		UpdateRequest up = new UpdateRequest();
	    DocumentObjectBinder binder = server.getBinder();
	    ArrayList<SolrInputDocument> docs =  new ArrayList<>(items.size());
        for (Item item : items) {
        	SolrInputDocument doc = binder.toSolrInputDocument(item);
        	docs.add(doc);
	    }
        up.add(docs);
		up.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
		up.setParam("update.chain", "langid");
		
		UpdateResponse response = up.process(server);
		return response.getStatus();
		
	}

	public Item get(String id) throws IOException, SolrServerException {
		SolrQuery query = new SolrQuery("id:" + id);
		QueryResponse rsp = server.query(query);
		
		List<Item> items = rsp.getBeans(Item.class);
        if(items == null || items.isEmpty()) {
        	return null;
        }
        
		return items.get(0);
	}
	
	public boolean update(String id, String field, Object value) throws IOException, SolrServerException {
				
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", id);
		
		Map<String, Object> map = new HashMap<>(1);
		map.put("set", value);
		doc.addField(field, map);
		
		server.add(doc);
		
		return true;
	}
	
	public boolean update(String id, Map<String, Object> fieldsToUpdate) throws IOException, SolrServerException {
		
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", id);
		
		for(Entry<String, Object> field : fieldsToUpdate.entrySet()) {
			Map<String, Object> map = new HashMap<>(1);
			map.put("set", field.getValue());
			doc.addField(field.getKey(), map);
		}
		
		server.add(doc);
		return true;
	}
	
	public boolean commit() {
		try {
			UpdateResponse response = server.commit();
			return (response.getStatus()==0) ? false : true;
		} catch (SolrServerException | IOException e) {
			return false;
		}
	}
	
	public List<Item> findSimilar(Item item, int k) {
		List<Item> items = new ArrayList<Item>();
		try {
			
			String textualQuery = getSimilarItemQuery(item);
			if(textualQuery == null)
				return items;
			
			SolrQuery query = new SolrQuery(textualQuery);
			query.setRows(k);
			System.out.println(query);
			
			return find(query);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return items;
	}
	
	public Set<String> findSimilarInRange(Item item, Date sinceDate, Date untilDate, int k) {
		Set<String> ids = new HashSet<String>();
		
		try {
			String textualQuery = getSimilarItemQuery(item);
			if(textualQuery == null) {
				return ids;
			}
			
			String since = sinceDate==null ? "*" : sdf.format(sinceDate);
			String until = untilDate==null ? "*" : sdf.format(untilDate);
			String timeQuery = "uploadDate:[" + since + " TO " + until + "]";
			
			SolrQuery query = new SolrQuery("(" + textualQuery + ") AND " + timeQuery);
			query.setRows(k);
			
			List<Item> items = find(query);
			if(items != null) {
				for(Item similarItem : items) {
					ids.add(similarItem.getId());
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return ids;
	}
	
	private String getSimilarItemQuery(Item item) {
		
		Set<String> terms = new HashSet<String>();
		TFIDFVector titleVector = item.getTitleVector();
		if(titleVector != null && !titleVector.isEmpty()) {
			terms.addAll(titleVector.keySet());
		}
		
		TFIDFVector descriptionVector = item.getDescriptionVector();
		if(descriptionVector != null && !descriptionVector.isEmpty()) {
			terms.addAll(descriptionVector.keySet());
		}
		
		TFIDFVector tagsVector = item.getTagsVector();
		if(tagsVector != null && !tagsVector.isEmpty()) {
			terms.addAll(tagsVector.keySet());
		}

		if(terms.isEmpty()) {
			return null;
		}
		else {
			List<String> queryParts = new ArrayList<String>();
			
			queryParts.add("title:(" + StringUtils.join(terms, " OR ") + ")");
			queryParts.add("description:(" + StringUtils.join(terms, " OR ") + ")");
			queryParts.add("tags:(" + StringUtils.join(terms, " OR ") + ")");
		
			return StringUtils.join(queryParts, " OR ");
		}
	}
	
	public List<Item> find(SolrQuery query) throws IOException, SolrServerException {
		List<Item> items = new ArrayList<Item>();
		try {
			QueryResponse rsp = server.query(query);
			items.addAll(rsp.getBeans(Item.class));
	        
			Map<String, TFIDFVector> titleVectors = getVectors(rsp, "title");
			Map<String, TFIDFVector> tagsVectors = getVectors(rsp, "tags");
			Map<String, TFIDFVector> descriptionVectors = getVectors(rsp, "description");
			
			for(Item item : items) {
			    String id = item.getId();
			    item.setTitleVector(titleVectors.get(id));
			    item.setTagsVector(tagsVectors.get(id));
			    item.setDescriptionVector(descriptionVectors.get(id));
			}
			
		}
		catch(Exception e) {
			e.printStackTrace();
			System.out.println("Exception: " + e.getMessage());
		}

		return items;
	}
	
	public List<Item> getInRange(Date sinceDate, Date untilDate) throws IOException, SolrServerException {
		
		List<Item> timeslotItems = new ArrayList<Item>();
		
		String since = (sinceDate==null) ? "*" : sdf.format(sinceDate);
		String until = (untilDate==null) ? "*" : sdf.format(untilDate);
		
		SolrQuery query = new SolrQuery("takenDate:[" + since + " TO " + until + "]");
		
		int rows = 100, start = 0;
		while(true) {
			//Paging 
			query.setStart(start * rows);
			query.setRows(rows);
			
			List<Item>  items = find(query);
			timeslotItems.addAll(items);
			
			if(items.isEmpty() || items.size()<rows) {
				break;
			}
			
			start++;
		}
		
		return timeslotItems;
	}
	
	public List<Item> findInRange(String textQuery, Date sinceDate, Date untilDate) throws IOException, SolrServerException {
		
		List<Item> timeslotItems = new ArrayList<Item>();
		
		String since = (sinceDate==null) ? "*" : sdf.format(sinceDate);
		String until = (untilDate==null) ? "*" : sdf.format(untilDate);
		
		
		SolrQuery query = new SolrQuery("(" + textQuery + ") AND takenDate:[" + since + " TO " + until + "]");
		
		int rows = 100, start = 0;
		while(true) {
			//Paging 
			query.setStart(start * rows);
			query.setRows(rows);
			
			List<Item>  items = find(query);
			timeslotItems.addAll(items);
			
			if(items.isEmpty() || items.size()<rows) {
				break;
			}
			
			start++;
		}
		
		return timeslotItems;
	}
	
	public void deleteAll() {
		try {
			server.deleteByQuery("*:*");
			server.commit();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	public void delete(String id) {
		try {
			server.deleteById(id);
			server.commit();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	public List<Pair<String, Long>> getFacets(String facetField, SolrQuery query) 
			throws SolrServerException, IOException {

		query.addFacetField(facetField);
	    QueryResponse rsp = server.query(query);
		 
		List<Pair<String, Long>> facets = new ArrayList<Pair<String, Long>>();
		FacetField facet = rsp.getFacetField(facetField);
		if(facet != null) {
			List<Count> facetValues = facet.getValues();
			for(Count count : facetValues) {
				facets.add(Pair.of(count.getName(), count.getCount()));
			}
		}
		
		return facets;	
	}

	public void loadItemVectors(Item item) throws SolrServerException, IOException {
		
		if(item.getDescriptionVector()!=null || item.getTitleVector() != null 
				|| item.getTagsVector() != null) {
			return;
		}
		
		SolrQuery query = new SolrQuery("id:" + item.getId());
		
		query.setRequestHandler("/tvrh");
		query.setParam("tv.all", true);
		try {
			QueryResponse rsp = server.query(query);
			
			if(item.getTitle() != null && !item.getTitle().isEmpty()) {
				Map<String, TFIDFVector> titleVectors = getVectors(rsp, "title");
				TFIDFVector titleVector = titleVectors.get(item.getId());
				if(titleVector != null) {
					item.setTitleVector(titleVector);
				}
			}
			
			if(item.getTags() != null && !item.getTags().isEmpty()) {
				Map<String, TFIDFVector> tagsVectors = getVectors(rsp, "tags");
				TFIDFVector tagsVector = tagsVectors.get(item.getId());
				if(tagsVector != null) {
					item.setTagsVector(tagsVector);
				}
			}
			
			if(item.getDescription() != null && !item.getDescription().isEmpty()) {
				Map<String, TFIDFVector> descriptionVectors = getVectors(rsp, "description");
				TFIDFVector descriptionVector = descriptionVectors.get(item.getId());
				if(descriptionVector != null) {
					item.setDescriptionVector(descriptionVector);
				}
			}
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}

	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Map<String, TFIDFVector> getVectors(QueryResponse rsp, String field) throws SolrServerException, IOException {
		
		Map<String, TFIDFVector> vectors = new HashMap<String, TFIDFVector>();
		NamedList<Object> response = rsp.getResponse();
		Object termVectors = response.get("termVectors");
		if(termVectors == null) {
			return vectors;
		}
		
		Iterator<Entry<String, Object>> termVectorsIterator =  ((NamedList) termVectors).iterator();
	    while(termVectorsIterator.hasNext()) {
	        Entry<String, Object> docTermVector = termVectorsIterator.next();
	        if(docTermVector.getKey().equals("uniqueKeyFieldName")) {
	        	continue;
	        }
	        	
	        String id = docTermVector.getKey();
	        TFIDFVector tfidfVector = new TFIDFVector();
	        for(Iterator<Entry<String, Object>> fi = ((NamedList)docTermVector.getValue()).iterator(); fi.hasNext(); ) {
	            Entry<String, Object> fieldEntry = fi.next();
	            if(fieldEntry.getKey().equals(field)) {
	                for(Iterator<Entry<String, Object>> tvInfoIt = ((NamedList)fieldEntry.getValue()).iterator(); tvInfoIt.hasNext(); ) {
	                	
	                    Entry<String, Object> tvInfo = tvInfoIt.next();
	                    String term = tvInfo.getKey();
	                    NamedList tv = (NamedList) tvInfo.getValue();
	                    
	                    TFIDF tfidf = new TFIDF();
	                    tfidf.tf = ((Integer) tv.get("tf")).doubleValue();
	                    tfidf.tfidf = (Double) tv.get("tf-idf");
	                    tfidf.idf = tfidf.tfidf / tfidf.tf;

	                    tfidfVector.addTerm(term, tfidf);
	                    
	                }
	            }       
	        }
	        vectors.put(id, tfidfVector);
	    }
	    return vectors;
	}
	
}