package gr.iti.mklab.yfcc.nlp;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.jsoup.Jsoup;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.util.logging.Redwood;
import edu.stanford.nlp.util.logging.StanfordRedwoodConfiguration;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.SparseMultigraph;
import gr.iti.mklab.yfcc.models.Item;
import gr.iti.mklab.yfcc.models.NamedEntity;
import gr.iti.mklab.yfcc.dao.SolrItemClient;

public class EntitiesExtractor {

	AbstractSequenceClassifier<CoreLabel> classifier;
	
	public EntitiesExtractor(String serializedClassifier) {
		StanfordRedwoodConfiguration.setup();
		Redwood.hideAllChannels();
		classifier = CRFClassifier.getClassifierNoExceptions(serializedClassifier);
	}
	
	public Map<String, NamedEntity> extractEntitiesFromItems(List<Item> items) {
		Map<String, NamedEntity> entities = new HashMap<String, NamedEntity>();
		for(Item item : items) {
			try {
				String text = item.getText();
				if(text != null && !text.equals("")) {
					extractEntities(text, entities);
				}
			} catch (Exception e) {
				//e.printStackTrace();
			}
		}
		return entities;
	}
	
	public Collection<NamedEntity> extractItemEntities(Item item) {
		Collection<NamedEntity> entities = new ArrayList<NamedEntity>();
		try {
			String text = item.getText();
			if((text == null || text.isEmpty() || text.length() < 10)) {
				return entities;
			}
				
			text = Jsoup.parse(text).text();
			text = text.replaceAll("&", " and ");
		
			entities.addAll(extractEntities(text));
		}
		catch(Exception e) {		
		}
		return entities;
		
	}
	
	public Collection<NamedEntity> extractEntities(String text) throws Exception {
		List<NamedEntity> entities = new ArrayList<NamedEntity>();
		
		Map<String, NamedEntity> entitiesMap = new HashMap<String, NamedEntity>();
		extractEntities(text, entitiesMap);
		
		entities.addAll(entitiesMap.values());
		
		return entities;
	}
	
	public void extractEntities(String text, Map<String, NamedEntity> entities) throws Exception {
		
		text = StringEscapeUtils.unescapeXml(text);
		String itemXML = classifier.classifyWithInlineXML(text);
		
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder docb = dbf.newDocumentBuilder();
        
        byte[] content = ("<DOC>" + itemXML + "</DOC>").getBytes();
		ByteArrayInputStream bis = new ByteArrayInputStream(content);
		Document doc = docb.parse(bis);
		
		//3-class model
		extractEntities(entities, doc, "PERSON");
		extractEntities(entities, doc, "LOCATION");
		extractEntities(entities, doc, "ORGANIZATION");
		
		//4-class model
		extractEntities(entities, doc, "MISC");
				
		//7-class model
		extractEntities(entities, doc, "TIME");
		extractEntities(entities, doc, "DATE");
		extractEntities(entities, doc, "MONEY");
		extractEntities(entities, doc, "PERCENT");
	}
	
	private void extractEntities(Map<String, NamedEntity> entities, Document doc, String type) {
		NodeList nodeList = doc.getElementsByTagName(type);

        for (int k = 0; k < nodeList.getLength(); k++) {
            String name = nodeList.item(k).getTextContent().toLowerCase();
            if(name == null) {
            	continue;
            }
            
            name = name.replaceAll("[^A-Za-z0-9 ]", "");
            name = name.replaceAll("\\s+", " ");
            name = name.trim();
            
            String key = type + "#" + name;
            
            if (!entities.containsKey(key)) {
            	NamedEntity e = new NamedEntity(name, type);
            	entities.put(key, e);
            }
            else {
            	NamedEntity e = entities.get(key);
            	e.incFrequency();
            	entities.put(key, e);
            }
        }
	}
	
	public static Graph<NamedEntity, Edge> getEntitiesGraph(Map<Long, List<NamedEntity>> entitiesPerStatus) {
		Graph<NamedEntity, Edge> graph = new SparseMultigraph<NamedEntity, Edge>();
		for(Entry<Long, List<NamedEntity>> entry : entitiesPerStatus.entrySet()) {
			List<NamedEntity> eColl = entry.getValue();
			for(int i=0; i<eColl.size(); i++) {
				for(int j=i+1; j<eColl.size(); j++) {
					NamedEntity e1 = eColl.get(i);
					NamedEntity e2 = eColl.get(j);
					graph.addVertex(e1);
					graph.addVertex(e2);
					
					Edge edge = graph.findEdge(e1, e2);
					if(edge == null)  {
						edge = new Edge();
						graph.addEdge(edge, e1, e2); 
					}
					else {
						edge.incFrequency();
					}
				}
			}
		}
		return graph;
	}
	
	public static class Edge {
		private Integer freq = 1;
		
		public void incFrequency() {
			freq++;
		}
		
		public Integer getFreaquency() {
			return freq;
		}
		
		public String toString() {
			return freq.toString();
		}
	}
	
	public static void main(String[] args) throws ParseException, IOException, SolrServerException {
		
		int timeslotLength = 48;
		
		String serializedClassifier3Class = "edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz";		
		EntitiesExtractor entitiesExtractor = new EntitiesExtractor(serializedClassifier3Class);
		
		SolrItemClient solrClient = new SolrItemClient("http://160.40.51.16:8983/solr/items");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date endDate = sdf.parse("2015-01-01T00:00:00Z");	
		Date sinceDate = sdf.parse("2006-01-01T00:00:00Z");
		Date untilDate = DateUtils.addHours(sinceDate, timeslotLength);	
				
		while(sinceDate.before(endDate)) {
			
			List<Item> items = solrClient.getInRange(sinceDate, untilDate);
			
			System.out.println(sinceDate + " - " + untilDate + " => " + items.size() + " items.");
			
			for(Item item : items) {
				
				String text = item.getText();
				try {
					if((text == null || text.isEmpty())) {
						continue;
					}
					
					text = Jsoup.parse(text).text();
					text = text.replaceAll("&", " and ");
			
					Collection<NamedEntity> entities = entitiesExtractor.extractEntities(text);
				
					List<String> persons = new ArrayList<String>();
					List<String> organizations = new ArrayList<String>();
					List<String> locations = new ArrayList<String>();	
					for(NamedEntity entity : entities) {
						if(entity.getType().equals("PERSON")) {
							persons.add(entity.getName());
						}
						else if(entity.getType().equals("ORGANIZATION")) {
							organizations.add(entity.getName());
						}
						else if(entity.getType().equals("LOCATION")) {
							locations.add(entity.getName());
						}
					}

					Map<String, Object> fieldsToUpdate = new HashMap<String, Object>();
					fieldsToUpdate.put("persons", persons);
					fieldsToUpdate.put("organizations", organizations);
					fieldsToUpdate.put("locations", locations);
					
					solrClient.update(item.getId(), fieldsToUpdate);
				}
				catch(Exception e) {
					
				}
			}
			
			solrClient.commit();
			
			sinceDate = DateUtils.addHours(sinceDate, timeslotLength);
			untilDate = DateUtils.addHours(sinceDate, timeslotLength);	
		}
		
	}

}
