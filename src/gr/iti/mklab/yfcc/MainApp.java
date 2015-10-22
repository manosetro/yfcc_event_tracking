package gr.iti.mklab.yfcc;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.solr.client.solrj.SolrServerException;

import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;
import gr.iti.mklab.yfcc.models.Item;
import gr.iti.mklab.yfcc.models.SameEventLink;
import gr.iti.mklab.yfcc.scan.ScanCommunityDetector;
import gr.iti.mklab.yfcc.scan.ScanCommunityStructure;
import gr.iti.mklab.yfcc.sedmodel.MultimodalClassifier;
import gr.iti.mklab.yfcc.dao.SolrItemClient;

public class MainApp {

	public static int timeslotLength = 24;
	
	public static void main(String...args) throws IOException, SolrServerException, ParseException {
		
		String modelFile = "/second_disk/workspace/yahoogc/models/textual.svm";
    	
		MultimodalClassifier classifier = new MultimodalClassifier(false);
		classifier.load(modelFile);
		
		SolrItemClient solrClient = new SolrItemClient("http://160.40.51.16:8983/solr/items");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		
		Date endDate = sdf.parse("2007-01-01T00:00:00Z");
		
		Date sinceDate = sdf.parse("2006-01-29T00:00:00Z");
		Date untilDate = DateUtils.addHours(sinceDate, timeslotLength);
		
		while(sinceDate.before(endDate)) {
			long t0 = System.currentTimeMillis();
			
			List<Item> items = solrClient.getInRange(sinceDate, untilDate);
			long t1 = System.currentTimeMillis();
			
			System.out.println(sinceDate + " - " + untilDate + " => " + items.size() + " items. Loaded in " + (t1-t0)/1000 + " secs.");
			
			Graph<String, SameEventLink> graph = new UndirectedSparseGraph<String, SameEventLink>();
			for(int i=0; i<items.size(); i++) {
				if(i%1000 == 0) {
					System.out.print(".");
				}
				
				Item item = items.get(i);
				
				solrClient.loadItemVectors(item);
				graph.addVertex(item.getId());
			}
			long t2 = System.currentTimeMillis();
			System.out.println("\nNodes added to graph in " + (t2-t1)/1000 + " secs.");
			
			//int k = (int) (0.05 * items.size());
			int positive = 0, negative = 0;
			for(int i=0; i<items.size(); i++) {				
				Item item = items.get(i);
				//Set<String> similarItems = solrClient.findSimilarInRange(item, sinceDate, untilDate, k);
				//for(Item candidateItem : similarItems) {
				for(int j=i+1; j<items.size(); j++) {
					Item item2 = items.get(j);
					
					double sameEventScore = classifier.test(item, item2);
					if(sameEventScore > 0) {
						positive++;
						SameEventLink e = new SameEventLink(sameEventScore);
						graph.addEdge(e, item.getId(), item2.getId());
					}
					else {
						negative++;
					}
				}
			}
			long t3 = System.currentTimeMillis();
			System.out.println("Edges added to graph in " + (t3-t2)/1000 + " secs.");
			System.out.println("Positive: " + positive + "\tNegative: " + negative);
					
			ScanCommunityDetector<String, SameEventLink> detector = new ScanCommunityDetector<String, SameEventLink>(0.7, 3);
	        ScanCommunityStructure<String, SameEventLink> structure = detector.getCommunityStructure(graph);
	        
	        long t4 = System.currentTimeMillis();
			System.out.println("SCAN ran in " + (t4-t3)/1000 + " secs.");
			
	        System.out.println("#communities: " + structure.getNumberOfCommunities());
	        System.out.println("#hubs: " + structure.getHubs().size());
	        System.out.println("#outliers: " + structure.getOutliers().size());
	        long t5 = System.currentTimeMillis();
			System.out.println("Total Time: " + (t5-t0)/1000 + " secs.");
	        System.out.println("=============================================");
	        
			sinceDate = DateUtils.addHours(sinceDate, timeslotLength);
			untilDate = DateUtils.addHours(sinceDate, timeslotLength);	
		}
		
	}
    
	
}
