package gr.iti.mklab.yfcc;

import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.graphs.build.ThreadedNNDescent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.server.UID;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.solr.client.solrj.SolrServerException;

import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;
import gr.iti.mklab.yfcc.dao.EventDAO;
import gr.iti.mklab.yfcc.dao.SolrEventClient;
import gr.iti.mklab.yfcc.dao.SolrItemClient;
import gr.iti.mklab.yfcc.models.Event;
import gr.iti.mklab.yfcc.models.Item;
import gr.iti.mklab.yfcc.models.SameEventLink;
import gr.iti.mklab.yfcc.scan.Community;
import gr.iti.mklab.yfcc.scan.ScanCommunityDetector;
import gr.iti.mklab.yfcc.scan.ScanCommunityStructure;
import gr.iti.mklab.yfcc.sedmodel.MultimodalClassifier;
import gr.iti.mklab.yfcc.utils.GraphUtils;
import gr.iti.mklab.yfcc.vindex.ServiceClient;
import gr.iti.mklab.yfcc.vocabulary.Vocabulary;

public class ApproximateApp {

	public static boolean useVisual;
	
	public static int timeslotLength;
	
	public static double structuralOverlapThreshold;
	public static double tagOverlapThreshold;
	public static int namedEntitiesThreshold;
	
	public static double densityThreshold;
	public static int minClusterSize;
	public static int hubAdjacentsThreshold;
	
	public static ExecutorService executorService = Executors.newFixedThreadPool(48);
	
	public static ServiceClient visualServiceClient;
	
	public static SolrItemClient itemClient;
	public static SolrEventClient eventClient;
	
	public static EventDAO dao;
	
	public static Vocabulary vocabulary;
	public static EventSummarizer summarizer = new EventSummarizer();
	
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	
	public static BlockingDeque<MultimodalClassifier> queue = new LinkedBlockingDeque<MultimodalClassifier>();

	private static String graphsDir;
	
	private static Date finalDate, sinceDate;
	
	public static void main(String...args) throws IOException, SolrServerException, ParseException {
		
		if(args.length > 0) {
			init(new File(args[0]));	
		}
		else {
			ClassLoader classLoader = ApproximateApp.class.getClassLoader();
			File file = new File(classLoader.getResource("file/test.xml").getFile());
			init(file);
		}
		
		Date untilDate = DateUtils.addHours(sinceDate, timeslotLength);
		
		Graph<Event, SameEventLink> eventsGraph = new UndirectedSparseGraph<Event, SameEventLink>();
		
		long total = 0, totalFiltered = 0, totalClustered = 0;
		
		Map<String, Item> itemsMap = new HashMap<String, Item>();
		Graph<String, SameEventLink> itemsGraph = new UndirectedSparseGraph<String, SameEventLink>();
		List<Item> previousItems = new ArrayList<Item>();
		while(sinceDate.before(finalDate)) {
			
			// Get items in time-slot
			long t0 = System.currentTimeMillis();
	
			List<Item> currentItems = itemClient.getInRange(sinceDate, untilDate);
			total += currentItems.size();
			
			long t1 = System.currentTimeMillis();
			System.out.println("[" + sinceDate + " - " + untilDate + "] => " + currentItems.size() + " items. Loaded in " + (t1-t0)/1000 + " secs.");		
			
			currentItems = loadItemsVectors(currentItems);
			totalFiltered += currentItems.size();
			System.out.println("Filtered items: " + currentItems.size());	
			
			// Load vlad vectors if setting uses visual features
			if(useVisual) {
				loadVisualFeatures(currentItems);
			}
			
			// Update items graph.
			// Add new nodes, add new edges.
			updateItemsGraph(currentItems, previousItems, itemsGraph, itemsMap);
			
			// approximate update of items graph
			//kNN(currentItems, previousItems, itemsGraph);
			
			long edges = itemsGraph.getEdgeCount();
			long vertices = itemsGraph.getVertexCount();
			System.out.println("#vertices: " + vertices + ",  #edges: " + edges);
			System.out.println("Density: " + GraphUtils.density(itemsGraph));
			
			// Find events in items graph
			List<Event> timeslotEvents = findEvents(itemsGraph, itemsMap);
	        System.out.println(timeslotEvents.size() + " events found in [" + sinceDate + " - " + untilDate + "]");
	        
	        // Merge events of this time-slot to active events
	        List<Event> newEvents = mergeEvents(timeslotEvents, eventsGraph);
	        System.out.println(newEvents.size() + " new events. " + (timeslotEvents.size()-newEvents.size()) + " merges.");
	        
	        // Remove inactive events from active events (no new inserts in the last time-slot)
	        List<Event> inactiveEvents = getInactiveEvents(eventsGraph, untilDate, 1);
	        System.out.println(inactiveEvents.size() + " events to be removed due to inactivity.");
	        
	        // summarize & save
			List<Event> eventsToSave = new ArrayList<Event>();
			for(Event event : inactiveEvents) {
				if(event.size() > minClusterSize && event.getDensity() > densityThreshold) {
		        	Map<String, Double> eventScores = summarizer.summarize(event);
		        	event.setEventScores(eventScores);
		        	
		        	eventsToSave.add(event);
				}
				eventsGraph.removeVertex(event);	
			}
			
	        dao.insert(eventsToSave);				// Save in MongoDB
	        eventClient.index(eventsToSave);		// Commit to SOLR
	        for(Event event : eventsToSave) {
	        	Graph<String, SameEventLink> eventGraph = event.getEventGraph();
	        	String gFile = graphsDir + event.getId() + ".graphml";
	        	GraphUtils.saveGraph(eventGraph, gFile);
		        
	        	totalClustered += event.size();
	        }
	        
	        // Add new events in the graph
	        for(Event newEvent : newEvents) {
	        	if(newEvent != null) {
	        		eventsGraph.addVertex(newEvent);
	        	}
	        	
	        	// TODO: Calculate same-event edges
	        	// Use Same Event SVM model to calculate a score of correlation between events
	        	// Possible Features: 
	        	//		start & end time, users overlap, geo-location (if exists), textual similarity, (optional: visual content)
	        }
	        // TODO: How to handle events graph? 
	        // 		1) SCAN clustering for event hierarchies
	        // 		2) Graph optimization -> Steiner Trees to find stories???
	        
	        System.out.println("Items Graph: " + itemsGraph.getVertexCount() + " vertices, " + itemsGraph.getEdgeCount() + " edges.");
	        
	        //Remove nodes of previous time-slot from the graph
	        for(Item item : previousItems) {
	        	String itemId = item.getId();
	        	itemsGraph.removeVertex(itemId);
	        	itemsMap.remove(itemId);
	        }
	        System.out.println("Items Graph after outdated removal: " + itemsGraph.getVertexCount() + " vertices, " + itemsGraph.getEdgeCount() + " edges.");
	        System.out.println("Items Map after outdated removal: " + itemsMap.size() + " items.");
	        
	        previousItems = currentItems;
	        
	        long activeItems = 0, maxItems = 0, avgItems = 0;
	        for(Event e : eventsGraph.getVertices()) {
	        	activeItems += e.getItemsMap().size();
	        	if(e.getItemsMap().size() > maxItems) {
	        		maxItems = e.getItemsMap().size();
	        	}
	        	avgItems += e.getItemsMap().size();
	        }
	        if(eventsGraph.getVertexCount() != 0) {
	        	avgItems = avgItems / eventsGraph.getVertexCount();
	        }
	        System.out.println("Events Graph: " + eventsGraph.getVertexCount() + " vertices, " + eventsGraph.getEdgeCount() + " edges.");
	        System.out.println("Active Items in events: " + activeItems + ", max items: " + maxItems + ", average items: " + avgItems);
	        		
	        filterEventsAndItems(eventsGraph, itemsGraph, itemsMap, previousItems);
	        
	        // Remove outliers
	        outliersRemoval(previousItems, itemsGraph, itemsMap);
	        System.out.println("Items Graph after outliers removal: " + itemsGraph.getVertexCount() + " vertices, " + itemsGraph.getEdgeCount() + " edges.");
	        System.out.println("Items Map after outliers removal: " + itemsMap.size() + " items.");
	        
	        System.out.println("Total proccesed items: " + total + ", kept: " + totalFiltered + ", discarded: " + (total-totalFiltered) + ", total clustered: " + totalClustered);
	        
	        long t6 = System.currentTimeMillis();
			System.out.println("Total Time: " + (t6-t0)/1000 + " secs.");
	        System.out.println("================================================================================================================");
	        
			sinceDate = DateUtils.addHours(sinceDate, timeslotLength);
			untilDate = DateUtils.addHours(sinceDate, timeslotLength);	
		}
		
		List<Event> eventsToSave = new ArrayList<Event>();
		for(Event event : eventsGraph.getVertices()) {
			if(event.size() > minClusterSize && event.getDensity() > densityThreshold) {
				Map<String, Double> eventScores = summarizer.summarize(event);
				event.setEventScores(eventScores);	
				
	        	double density = GraphUtils.density(event.getEventGraph());
	        	event.setDensity(density);
	        	
				eventsToSave.add(event);
			}	
		}
		
		dao.insert(eventsToSave);
		eventClient.index(eventsToSave);
        for(Event event : eventsToSave) {
        	Graph<String, SameEventLink> eventGraph = event.getEventGraph();
        	File gFile = new File(graphsDir, event.getId() + ".graphml");
        	GraphUtils.saveGraph(eventGraph, gFile);
        }
        
		// Prepare to shutdown
		executorService.shutdown();
	}
	
	public static void init(File propsFile) throws IOException, ParseException {
		FileInputStream inStream = new FileInputStream(propsFile);
		Properties properties = new Properties();
		properties.load(inStream);
		
		useVisual = Boolean.parseBoolean((String) properties.getOrDefault("use_visual", "false"));
		
		String mongoHost = (String) properties.getOrDefault("mongo_host", "127.0.0.1");
		String mongoDatabase = (String) properties.get("mongo_database");
		dao = EventDAO.getDAO(mongoHost, mongoDatabase);
		
		String solrItemsCollection = (String) properties.get("solr_items_collection");
		itemClient = new SolrItemClient(solrItemsCollection);
		
		String solrEventsCollection = (String) properties.get("solr_events_collection");
		eventClient = new SolrEventClient(solrEventsCollection);
		
		String visualService = (String) properties.get("visual_service");
		visualServiceClient = new ServiceClient(visualService);
		 
		graphsDir = (String) properties.get("graphs_dir");
		timeslotLength = Integer.parseInt((String) properties.getOrDefault("timeslot_length", "24"));
		structuralOverlapThreshold = Double.parseDouble((String) properties.getOrDefault("structural_overlap_threshold", "0.5"));
		tagOverlapThreshold = Double.parseDouble((String) properties.getOrDefault("tags_overlap_threshold", "0.9"));
		namedEntitiesThreshold = Integer.parseInt((String) properties.getOrDefault("named_entities_threshold", "5"));
		densityThreshold = Double.parseDouble((String) properties.getOrDefault("density_threshold", "0.3"));
		minClusterSize = Integer.parseInt((String) properties.getOrDefault("min_cluster_size", "15"));
		hubAdjacentsThreshold = Integer.parseInt((String) properties.getOrDefault("hub_adjacents_threshold", "15"));
		
		// Load Classifiers 
		String classifierModel = useVisual ? "textual_visual.svm" : "textual.svm";
		for(int k=0; k<100; k++) {
			MultimodalClassifier classifier = new MultimodalClassifier(properties.get("classifiers_directory") + classifierModel, useVisual);
			queue.add(classifier);
		}
		
		vocabulary = Vocabulary.loadFromFile((String) properties.get("vocabulary_file"));
		System.out.println("Vocabulary loaded: " + vocabulary.documents() + " docs, " + vocabulary.size() + " terms");		
		
		finalDate = sdf.parse((String) properties.get("final_date"));
		sinceDate = sdf.parse((String) properties.get("since_date"));
		
		System.out.println("Detect and track events from " + sinceDate +  " to " + finalDate);
	}
	
	public static int outliersRemoval(List<Item> items, Graph<String, SameEventLink> itemsGraph, Map<String, Item> itemsMap) {
        
        List<Item> outliersToRemove = new ArrayList<Item>();
        for(Item item : items) {
        	String itemId = item.getId();
        	if(itemsGraph.getNeighborCount(itemId) == 0) {
        		outliersToRemove.add(item);
        	}
        }
        
        for(Item item : outliersToRemove) {
        	items.remove(item);
        	String itemId = item.getId();
        	itemsGraph.removeVertex(itemId);
        	itemsMap.remove(itemId);
        }
        
        return outliersToRemove.size();
	}
	
	public static List<Event> getInactiveEvents(Graph<Event, SameEventLink> eventsGraph, Date untilDate, int n) {
		List<Event> inactiveEvents = new ArrayList<Event>();
		
		Date activeDateThreshold = DateUtils.addHours(untilDate, -n * timeslotLength);
		System.out.println("Remove events inactive since " + activeDateThreshold);
		for(Event activeEvent : eventsGraph.getVertices()) {
			Date lastDate  = new Date(activeEvent.getMaxTakenTime()); 
			if(lastDate.before(activeDateThreshold)) {
				inactiveEvents.add(activeEvent);
			}
		}
		return inactiveEvents;
	}
	
	public static void filterEventsAndItems(Graph<Event, SameEventLink> eventsGraph, Graph<String, SameEventLink> itemsGraph,
			Map<String, Item> itemsMap, List<Item> previousItems) {
		
		// Filter events of low density
        List<Event> eventsToRmv = new ArrayList<Event>();
        for(Event event : eventsGraph.getVertices()) {
        	if(event.getDensity() <= densityThreshold) {
        		// remove event due to low density 
        		eventsToRmv.add(event);
        	}
        }
        
        if(!eventsToRmv.isEmpty()) {
        	for(Event eventToRemove : eventsToRmv) {
        		eventsGraph.removeVertex(eventToRemove);
        		
        		Map<String, Item> eventItems = eventToRemove.getItemsMap();
        		if(eventItems != null) {
        			for(Item item : eventItems.values()) {
        				previousItems.remove(item);
        	        	String itemId = item.getId();
        	        	itemsGraph.removeVertex(itemId);
        	        	itemsMap.remove(itemId);
        			}
        		}
        	}
        
        	int activeItems = 0;
        	int maxItems = 0;
        	int avgItems = 0;
        	for(Event e : eventsGraph.getVertices()) {
        		activeItems += e.getItemsMap().size();
        		if(e.getItemsMap().size() > maxItems) {
        			maxItems = e.getItemsMap().size();
        		}
        		avgItems += e.getItemsMap().size();
        	}
        	if(eventsGraph.getVertexCount() != 0) {
	        	avgItems = avgItems / eventsGraph.getVertexCount();
	        }
	        System.out.println("Items Graph after events filtering: " + itemsGraph.getVertexCount() + " vertices, " + itemsGraph.getEdgeCount() + " edges.");
	        System.out.println("Items Map after events filtering: " + itemsMap.size() + " items.");
        	System.out.println("Events Graph after filtering: " + eventsGraph.getVertexCount() + " vertices, " + eventsGraph.getEdgeCount() + " edges.");
        	System.out.println("Active Items in events: " + activeItems + ", max items: " + maxItems + ", average items: " + avgItems);
        }
	}
	
	public static void updateItemsGraph(List<Item> currentItems, List<Item> previousItems, Graph<String, SameEventLink> itemsGraph, Map<String, Item> itemsMap) {
		
		long t1 = System.currentTimeMillis();
		
		// Add items to map & graph
		int vertices = 0;
		for(Item item : currentItems) {
			vertices++;
			itemsGraph.addVertex(item.getId());
			itemsMap.put(item.getId(), item);
		}
		
		long t2 = System.currentTimeMillis();
		System.out.println(vertices + " nodes added to graph in " + (t2 - t1)/1000 + " secs.");

		System.out.println(queue.size() + " classifiers available.");
		
		// Add Same Event edges
		LinkedList<Future<Integer>> futures = new LinkedList<Future<Integer>>();
		int edges = itemsGraph.getEdgeCount();
		for(int i = 0; i<currentItems.size()-1; i++) {				
			Item item1 = currentItems.get(i);
			
			for(int j = i+1; j<currentItems.size(); j++) {
				Item item2 = currentItems.get(j);
				
				if(!item1.hasMachineTags() && !item2.hasMachineTags() && !item1.sameLanguage(item2)) {
					continue;
				}
				
				SECalculationTask task = new SECalculationTask(item1, item2, itemsGraph);
				Future<Integer> future = executorService.submit(task);
				futures.addLast(future);
			}
		}
		System.out.println(futures.size() + " inter-timeslot pairs to compare.");
		waitTasks(futures);
		
        // Remove outliers
        int outliers = outliersRemoval(currentItems, itemsGraph, itemsMap);
        System.out.println(outliers + " removed as outliers");
        
		long t3 = System.currentTimeMillis();
		System.out.println((itemsGraph.getEdgeCount() - edges) + " edges for new nodes added to graph in " + (t3 - t2)/1000 + " secs.");
		edges = itemsGraph.getEdgeCount();
		for(int i = 0; i<previousItems.size(); i++) {				
			Item item1 = previousItems.get(i);
			
			for(int j = 0; j<currentItems.size(); j++) {
				Item item2 = currentItems.get(j);
				if(!item1.hasMachineTags() && !item2.hasMachineTags() && !item1.sameLanguage(item2)) {
					continue;
				}
				
				SECalculationTask task = new SECalculationTask(item1, item2, itemsGraph);
				try {
					Future<Integer> future = executorService.submit(task);
					futures.addLast(future);
				} 
				catch(RejectedExecutionException | NullPointerException e) {
					System.err.println("Exception during task submission: " + task);
					e.printStackTrace();
				}
				
			}
		}
		System.out.println(futures.size() + " intra-timeslot pairs to compare.");
		if(!futures.isEmpty()) {
			waitTasks(futures);
		}
		
		long t4 = System.currentTimeMillis();
		System.out.println((itemsGraph.getEdgeCount() - edges) + " edges between old and new nodes added to graph in " + (t4 - t3)/1000 + " secs.");
				
	}
	
	
	public static void loadVisualFeatures(List<Item> currentItems) {
		LinkedList<Future<Integer>> futures = new LinkedList<Future<Integer>>();
		for(Item item : currentItems) {
			VladRequestTask task = new VladRequestTask(item);
			Future<Integer> future = executorService.submit(task);
			futures.addLast(future);
		}
		
		if(!futures.isEmpty()) {
			waitTasks(futures);
		}
		
		List<Item> tbRmvd = new ArrayList<Item>();
		for(Item item : currentItems) {	
			if(item.getUrl() == null || (useVisual && item.getVlad() == null)) {
				tbRmvd.add(item);
			}
		}
		currentItems.removeAll(tbRmvd);
	}
	
	public static List<Event> findEvents(Graph<String, SameEventLink> itemsGraph, Map<String, Item> itemsMap) {
		
		long t1 = System.currentTimeMillis();
		
		// e = 0.65, mu = 5
		ScanCommunityDetector<String, SameEventLink> detector = new ScanCommunityDetector<String, SameEventLink>(0.65, 5);
        ScanCommunityStructure<String, SameEventLink> structure = detector.getCommunityStructure(itemsGraph);
        
        long t2 = System.currentTimeMillis();
		System.out.println("SCAN ran in " + (t2 - t1)/1000 + " secs.");
        System.out.println("#communities: " + structure.getNumberOfCommunities());
        System.out.println("#hubs: " + structure.getHubs().size());
        System.out.println("#outliers: " + structure.getOutliers().size());
        
        int clustered = structure.getNumberOfMembers();
        System.out.println("#clustered: " + clustered);
        
        // Discard outliers & attach hubs to multiple clusters
        for(String hub : structure.getHubs()) {
        	structure.getHubAdjacentCommunities(hub);
        	for(int communityId = 0; communityId < structure.getNumberOfCommunities(); communityId++) {
	        	Community<String, SameEventLink> community = structure.getCommunity(communityId);
	        	if(community == null) {
	        		//System.err.println("Community #" + communityId + " is null.");
	        		continue;
	        	}
	        	
	        	List<String> members = community.getMembers();
	        	if(members != null && !members.isEmpty()) {
	        		int adjacents = 0;
	        		for(String memberId : members) {
	        			if(itemsGraph.findEdge(hub, memberId) != null || itemsGraph.findEdge(memberId, hub) != null) {
	        				adjacents++;
	        			}
	        		}
	        		
	        		if(adjacents > hubAdjacentsThreshold) {
	        			structure.addVertexToCommunity(hub, communityId);
	        		}
	        	}
        	}
        }
        System.out.println("#clustered after hub intergation: " + structure.getNumberOfMembers() + ", attached hubs: " + (structure.getNumberOfMembers()-clustered));
        
        List<Event> timeslotEvents = new ArrayList<Event>();
        for(int cId = 0; cId < structure.getNumberOfCommunities(); cId++) {
        	Community<String, SameEventLink> community = structure.getCommunity(cId);
        	if(community == null) {
        		//System.err.println("Community #" + cId + " is null.");
        		continue;
        	}
        	
        	Event event = new Event();
        	UID uid = new UID();
        	event.setId(uid.toString());
        	
        	List<String> members = community.getMembers();
        	if(members == null) {
        		//System.out.println("Community Members of #" + cId + " is null.");
        		continue;
        	}
        	
        	for(String memberId : members) {
        		Item memberItem = itemsMap.get(memberId);
        		if(memberItem != null) {
        			event.addItem(memberItem);
        		}
        		else {
        			System.out.println(memberId + " items is not exist");
        		}
        	}
        	Graph<String, SameEventLink> eventGraph = GraphUtils.filter(itemsGraph, members);
        	event.setEventGraph(eventGraph);
        	 
        	double density = GraphUtils.density(eventGraph);
        	event.setDensity(density);
        	
        	if(event.getDensity() > densityThreshold) {
        		timeslotEvents.add(event);
        	}
        }
        return timeslotEvents;
	}
	
	/*
	 * Return new events. 
	 */
	public static List<Event> mergeEvents(List<Event> timeslotEvents, Graph<Event, SameEventLink> eventsGraph) {
        
		int overlaps = 0, tagsOverlaps = 0, neOverlaps = 0;
		List<Event> newEvents = new ArrayList<Event>();
        for(Event timeslotEvent : timeslotEvents) {
        	timeslotEvent.updateTextualContent();
    		
        	List<Event> eventsToMerge = new ArrayList<Event>();
        	for(Event activeEvent : eventsGraph.getVertices()) {       		
        		// structural overlap
        		double overlap = timeslotEvent.overlap(activeEvent);	
        		if(overlap > structuralOverlapThreshold) {
        			overlaps++;
        			eventsToMerge.add(activeEvent);
        		}
        		else if(timeslotEvent.tagsOverlap(activeEvent) > tagOverlapThreshold) {
        			// tags similarity
        			tagsOverlaps++;
        			eventsToMerge.add(activeEvent);
        		}
        		else if(timeslotEvent.namedEntitiesOverlap(activeEvent).size() > namedEntitiesThreshold) {
        			// overlap of named entities between events
        			neOverlaps++;
        			eventsToMerge.add(activeEvent);
        		}
 	        }
        	
        	if(eventsToMerge.isEmpty()) {
        		// There in no previous similar event. 
        		// Create new Event and add it to new events list.
        		newEvents.add(timeslotEvent);
        	}
        	else {
        		Event activeEvent = eventsToMerge.get(0);
        		for(int eventIndex = 1; eventIndex<eventsToMerge.size(); eventIndex++) {
        			Event eventToMerge = eventsToMerge.get(eventIndex);
        			activeEvent.merge(eventToMerge);
        			
        			// remove merged event from events graph
        			eventsGraph.removeVertex(eventToMerge);
        			// TODO: update edges 	        			
        		}
        		activeEvent.merge(timeslotEvent);
        		activeEvent.updateTextualContent();
        		
            	double density = GraphUtils.density(activeEvent.getEventGraph());
            	activeEvent.setDensity(density);
            	
        	}
        }
        
        System.out.println("Merges: \n\t" 
        		+ overlaps + " due to structural overlap.\n\t"
        		+ tagsOverlaps + " due to tags overlap.\n\t"
        		+ neOverlaps + " due to named entities overlap."
        		);
        
        return newEvents;
	}
	
	public static List<Item> loadItemsVectors(List<Item> currentItems) {
		List<Item> items = new ArrayList<Item>();
		for(int i = 0; i<currentItems.size(); i++) {
			if(i%1000 == 0) {
				System.out.print(".");
			}
		
			Item item = currentItems.get(i);
			
			// filtering
			if(item.getUrl() == null) {
				continue;
			}
			
			if(!item.hasMachineTags() && (item.getTitle() == null || item.getTitle().length()<10 
					|| item.getTitleTerms() == null || item.getTitleTerms().size() < 3)
					&& (item.getTags() == null || item.getTags().size() < 3)) {
				continue;
			}
			
			if(item.hasMachineTags() || (item.getDescription() != null && item.getDescription().length() > 12 
					&& item.getDescriptionTerms() != null && item.getDescriptionTerms().size() > 3) || 
					(item.getTags() != null && item.getTags().size() > 4)) {
				
				vocabulary.loadItemVectors(item);
				items.add(item);
			}
			
		}
		return items;
	}
	
	public static void waitTasks(List<Future<Integer>> futures) {
		int i = 50000, j=5000000;
		int r = 0;
		int positive = 0, negative = 0;
		while(!futures.isEmpty()) {
			List<Future<Integer>> tbRmvd = new ArrayList<Future<Integer>>();
			for(Future<Integer> future : futures) {
				if(future.isDone() || future.isCancelled()) {
					try {
						Integer result = future.get();
						if(result > 0) {
							positive++;
						}
						else {
							negative++;
						}
					} catch (Exception e) { 
						System.err.println("Exception while waiting tasks to complete: " + e.getMessage());
					}
					tbRmvd.add(future);
				}
			}
			
			for(Future<Integer> future : tbRmvd) {
				r++;
				if(r%i==0) {
					System.out.print(".");
					if(r%j==0) {
						System.out.println(" (" + r + ")");
					}
				}
				futures.remove(future);
			}
			
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println(" (" + r + ")\n #Positives: " + positive + ", #Negatives:" + negative);
	}

	public static class VladRequestTask implements Callable<Integer> {

		private Item item;

		public VladRequestTask(Item item) { 
			this.item = item;
		}
	
		@Override
		public Integer call() throws Exception {		
			try {
				String id = item.getId();
				String url = item.getUrl();
				if(url == null || id == null) {
					return 0;
				}
				
				double[] vlad = visualServiceClient.getVector(id, url);
				item.setVlad(vlad);
				
				return vlad==null? 0 : 1;
			}
			catch(Exception e) {
				return 0;
			}
		}
	}
	
	public static class SECalculationTask implements Callable<Integer> {
		
		private Item item1, item2;
		private Graph<String, SameEventLink> graph;

		public SECalculationTask(Item item1, Item item2, Graph<String, SameEventLink> graph) {
			this.item1 = item1;
			this.item2 = item2;
			
			this.graph = graph;
		}
		
		@Override
		public  Integer call() {
			int returnedValue = -1;
			MultimodalClassifier classifier = null;
			try {
				if(item1.sameMachineTags(item2)) {
					SameEventLink edge = new SameEventLink(1);
					synchronized(graph) {
						graph.addEdge(edge, item1.getId(), item2.getId());
					}
					returnedValue = 1;
				}
				else {
					classifier = queue.takeFirst();
					if(useVisual && (item1.getVlad() == null || item2.getVlad() == null)) {
						returnedValue = -1;
					}
					else {
						double score = classifier.test(item1, item2);
						if(score > 0) {
							SameEventLink edge = new SameEventLink(score);
							synchronized(graph) {
								graph.addEdge(edge, item1.getId(), item2.getId());
							}
							returnedValue = 1;
						}
						else {
							// same event if items have more than 4 common named entities
							Set<String> neOverlap = item1.namedEntitiesOverlap(item2);
							if(neOverlap != null && neOverlap.size() > 5) {
								SameEventLink edge = new SameEventLink(1);
								synchronized(graph) {
									graph.addEdge(edge, item1.getId(), item2.getId());
								}
								returnedValue = 1;
							}
						}
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				if(classifier != null) {
					try {
						queue.putLast(classifier);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			return returnedValue;
		}

		
		public String toString() {
			return "<" + item1.getId() + " - " + item2.getId() + ">";
		}
	}
	
	public static void kNN(List<Item> currentItems, List<Item> previousItems, Graph<String, SameEventLink> itemsGraph) {
       
		long t = System.currentTimeMillis();
		int edges = itemsGraph.getEdgeCount();
		
		System.out.println("Run approximate kNN for items graph update.");
		
		System.out.println(queue.size() + " classifiers available.");
		
        List<Node<Item>> nodes = new ArrayList<Node<Item>>(currentItems.size());
        for (Item item : currentItems) {
        	Node<Item> node = new Node<Item>(item.getId(), item);
            nodes.add(node);
        }
        for (Item item : previousItems) {
        	Node<Item> node = new Node<Item>(item.getId(), item);
            nodes.add(node);
        }
        System.out.println((nodes.size() * nodes.size()) + " pairs to compare.");
        
        SimilarityInterface<Item> similarityFunction = new SimilarityInterface<Item>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public double similarity(Item item1, Item item2) {
				
				double returnedValue = 0;
				if(!item1.hasMachineTags() && !item2.hasMachineTags() && !item1.sameLanguage(item2)) {
					return returnedValue;
				}
				
				if(itemsGraph.containsVertex(item1.getId()) && itemsGraph.containsVertex(item2.getId())) {
					SameEventLink edge = itemsGraph.findEdge(item1.getId(), item2.getId());
					if(edge == null) {
						return returnedValue;
					}
					return edge.weight;
				}
				
				if(item1.sameMachineTags(item2)) {
					returnedValue = 1.;
				}
				else {
					MultimodalClassifier classifier = null;
					try {
						classifier = queue.takeFirst();
						if(useVisual && (item1.getVlad() == null || item2.getVlad() == null)) {
							returnedValue = 0;
						}
						else {
							double score = classifier.test(item1, item2);
							if(score > 0) {
								returnedValue = score;
							}
							else {
								// same event if items have more than 4 common named entities
								Set<String> neOverlap = item1.namedEntitiesOverlap(item2);
								if(neOverlap != null && neOverlap.size() > 5) {
									returnedValue = 1;
								}
							}
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					finally {
						if(classifier != null) {
							try {
								queue.putLast(classifier);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					}
				}
                return returnedValue;
            }
        };

        int k = (int) (0.05 * nodes.size());
        
        // Instantiate and configure the algorithm
        ThreadedNNDescent<Item> builder = new ThreadedNNDescent<Item>();
        builder.setThreadCount(16);
        builder.setK(k);
        builder.setSimilarity(similarityFunction);
        builder.setMaxIterations(50);
        builder.setDelta(0.05);
        builder.setRho(0.5);

        // Run the algorithm and get computed neighbor lists
        Map<Node<Item>, NeighborList> graph = builder.computeGraph(nodes);
        for (Node<Item> node : nodes) {
        	String id1 = node.id;
        	if(!itemsGraph.containsVertex(id1)) {
        		itemsGraph.addVertex(id1);
        	}
        	
            NeighborList nl = graph.get(node);
            Iterator<Neighbor> it = nl.iterator();
        	while(it.hasNext()) {
        		Neighbor neighbor = it.next();
        		String id2 = neighbor.node.id;
        		if(!itemsGraph.containsVertex(id2)) {
            		itemsGraph.addVertex(id2);
            	}
        		
        		SameEventLink edge = itemsGraph.findEdge(id1, id2);
        		if(edge == null) {
        			if(neighbor.similarity > 0) {
        				edge = new SameEventLink(neighbor.similarity);
        				itemsGraph.addEdge(edge, id1, id2);
        			}
        		}
        	}
            
        }
        
        long t2 = System.currentTimeMillis();
		System.out.println((itemsGraph.getEdgeCount() - edges) + " edges between nodes added to graph in " + (t2 - t)/1000 + " secs.");
		
    }

}
