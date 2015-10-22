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

import gr.iti.mklab.yfcc.models.Event;
import gr.iti.mklab.yfcc.models.TFIDF;
import gr.iti.mklab.yfcc.models.TFIDFVector;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;

public class SolrEventClient {
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	
	private HttpSolrClient server;
	
	public SolrEventClient(String solrUrl) {
		server = new HttpSolrClient(solrUrl);
	}
	
	public void index(Event event) throws IOException, SolrServerException {
		server.addBean(event);
		server.commit();
	}
	
	public int index(List<Event> events) throws IOException, SolrServerException {
		UpdateRequest up = new UpdateRequest();
	    //DocumentObjectBinder binder = server.getBinder();
	    ArrayList<SolrInputDocument> docs =  new ArrayList<>(events.size());
        for (Event event : events) {
        	//SolrInputDocument doc = binder.toSolrInputDocument(event);
        	SolrInputDocument doc = event.toSolrInputDocument();
        	docs.add(doc);
	    }
        
        up.add(docs);
		up.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
		up.setParam("update.chain", "langid");
		
		UpdateResponse response = up.process(server);
		server.commit();
		return response.getStatus();
		
	}

	public Event get(String id) throws IOException, SolrServerException {
		SolrQuery query = new SolrQuery("id:" + id);
		QueryResponse rsp = server.query(query);
		
		List<Event> events = rsp.getBeans(Event.class);
        if(events == null || events.isEmpty()) {
        	return null;
        }
        
		return events.get(0);
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
	
	public List<Event> findSimilar(Event event, int k) {
		List<Event> items = new ArrayList<Event>();
		try {
			
			String textualQuery = getSimilarEventQuery(event);
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
	
	public List<Event> findSimilarInRange(Event item, Date sinceDate, Date untilDate, int k) {
		List<Event> events = new ArrayList<Event>();
		try {
			String textualQuery = getSimilarEventQuery(item);
			if(textualQuery == null) {
				return events;
			}
			
			String since = sinceDate==null ? "*" : sdf.format(sinceDate);
			String until = untilDate==null ? "*" : sdf.format(untilDate);
			
			String timeQuery = "uploadDate:[" + since + " TO " + until + "]";
			
			SolrQuery query = new SolrQuery("(" + textualQuery + ") AND " + timeQuery);
			query.setRows(k);
			
			return find(query);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return events;
	}
	
	private String getSimilarEventQuery(Event event) {
		
		Set<String> terms = new HashSet<String>();
		TFIDFVector titleVector = event.getTitleVector();
		
		if(titleVector != null && !titleVector.isEmpty()) {
			terms.addAll(titleVector.keySet());
		}

		if(terms.isEmpty()) {
			return null;
		}
		else {
			List<String> queryParts = new ArrayList<String>();
			
			queryParts.add("title:(" + StringUtils.join(terms, " ") + ")");
			queryParts.add("description:(" + StringUtils.join(terms, " ") + ")");
			queryParts.add("tags:(" + StringUtils.join(terms, " ") + ")");
		
			return StringUtils.join(queryParts, " OR ");
		}
	}
	
	public List<Event> find(SolrQuery query) throws IOException, SolrServerException {
		List<Event> events = new ArrayList<Event>();
		try {
			QueryResponse rsp = server.query(query);
			events.addAll(rsp.getBeans(Event.class));
		}
		catch(Exception e) {
			e.printStackTrace();
			System.out.println("Exception: " + e.getMessage());
		}

		return events;
	}
	
	public List<Event> getInRange(Date sinceDate, Date untilDate) throws IOException, SolrServerException {
		
		List<Event> timeslotEvents = new ArrayList<Event>();
		
		String since = (sinceDate==null) ? "*" : sdf.format(sinceDate);
		String until = (untilDate==null) ? "*" : sdf.format(untilDate);
		
		SolrQuery query = new SolrQuery("takenDate:[" + since + " TO " + until + "]");
		
		int rows = 100, start = 0;
		while(true) {
			//Paging 
			query.setStart(start * rows);
			query.setRows(rows);
			
			List<Event> events = find(query);
			timeslotEvents.addAll(events);
			
			if(events.isEmpty() || events.size()<rows) {
				break;
			}
			
			start++;
		}
		
		return timeslotEvents;
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

	public static void main(String...args) {
		SolrEventClient eventClient = new SolrEventClient("http://160.40.51.16:8983/solr/events_2");
		eventClient.deleteAll();
	}
}
