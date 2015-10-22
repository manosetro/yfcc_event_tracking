package gr.iti.mklab.yfcc.models;

import edu.uci.ics.jung.graph.Graph;
import gr.iti.mklab.yfcc.clustering.spatial.ExtGrahamScan;
import gr.iti.mklab.yfcc.structures.ItemsTimeline;
import gr.iti.mklab.yfcc.utils.GeodesicDistanceCalculator;
import gr.iti.mklab.yfcc.utils.GraphUtils;
import gr.iti.mklab.yfcc.utils.VincentyDistance;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.solr.common.SolrInputDocument;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Transient;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class Event {
	
	@Id
	private String id;
	
	private Long meanTakenTime;
	private Long medianTakenTime;

	private Date maxTakenTime = new Date(0L);
	private Date minTakenTime = new Date(Long.MAX_VALUE);
	
    private Point meanGeo = null;
    private Point medianGeo = null;
    
    private ItemsTimeline timeline = new ItemsTimeline(1, TimeUnit.HOURS);
    private Graph<String, SameEventLink> eventGraph;
    private double density = 0;
    
    private Map<String, Double> eventScores;
    
    @Transient
	private TFIDFVector titleVector = new TFIDFVector();
    @Transient
	private TFIDFVector tagsVector = new TFIDFVector();
    @Transient
	private TFIDFVector descriptionVector = new TFIDFVector();
	
    private Double eventGeoRange = 0.0;
    
	@Transient
	private Map<String, Item> items = new HashMap<String, Item>();
	
	private Set<String> users = new HashSet<String>();
	
	private Map<String, Integer> tags = new HashMap<String, Integer>();
	private Set<String> machineTags = new HashSet<String>();
	
	// Named Entities
	private Map<String, Integer> persons = new HashMap<String, Integer>();
	private Map<String, Integer> locations = new HashMap<String, Integer>();
	private Map<String, Integer> organizations = new HashMap<String, Integer>();
	
	private int merges = 0;
	private Set<String> mergedEvents = new HashSet<String>();
	
	private BasicDBObject obj;
    
	public Event() {
		
	}
	
	public Event(String id) {
		this.id = id;
	}

    public void addItem(Item item) { 
    		 
    	if(items.containsKey(item.getId())) {
    		return;
    	}
    	
    	items.put(item.getId(), item); 	
    	
    	if(item.getTags() != null) {
    		for(String tag : item.getTags()) {
    			Integer count = this.tags.get(tag);
    			if(count == null) {
    				count = 1;
    			}
    			else {
    				count++;
    			}
    			this.tags.put(tag, count);
    		}
    	}
    	
    	if(item.getMachineTags() != null) {
    		this.machineTags.addAll(item.getMachineTags());
    	}
    	
    	if(item.getPersons() != null) {
    		for(String person : item.getPersons()) {
    			Integer count = this.tags.get(person);
    			if(count == null) {
    				count = 1;
    			}
    			else {
    				count++;
    			}
    			this.persons.put(person, count);
    		}
    	}

    	if(item.getLocations() != null) {
    		for(String location : item.getLocations()) {
    			Integer count = this.tags.get(location);
    			if(count == null) {
    				count = 1;
    			}
    			else {
    				count++;
    			}
    			this.locations.put(location, count);
    		}
    	}
    	
    	if(item.getOrganizations() != null) {
    		for(String organization : item.getOrganizations()) {
    			Integer count = this.tags.get(organization);
    			if(count == null) {
    				count = 1;
    			}
    			else {
    				count++;
    			}
    			this.organizations.put(organization, count);
    		}
    	}
    	
    	TFIDFVector titleVector = item.getTitleVector();
    	if(titleVector != null && !titleVector.isEmpty()) {
    		this.titleVector.mergeVector(titleVector);
    	}

    	TFIDFVector tagsVector = item.getTagsVector();
    	if(tagsVector != null && !tagsVector.isEmpty()) {
    		this.tagsVector.mergeVector(tagsVector);
    	}

    	TFIDFVector descVector = item.getDescriptionVector();
    	if(descVector != null && !descVector.isEmpty()) {
    		this.descriptionVector.mergeVector(descVector);
    	}
    	
    	Date takenDate = item.getTakenDate();
    	
    	if(takenDate.before(minTakenTime) && takenDate.getTime() != 0) {
    		minTakenTime.setTime(takenDate.getTime());
    	}
    	
    	if(takenDate.after(maxTakenTime)) {
    		maxTakenTime.setTime(takenDate.getTime());
    	}
    	
    	if(item.getUserid() != null) {
    		users.add(item.getUserid());
    	}
    	
    	timeline.put(item.getId(), item.getTakenDate());
    }
    
    public void addItems(List<Item> items) { 	
    	for(Item item : items) {
    		addItem(item);
    	}
    }
    
    public String getId() {
    	return id;
    }
    
    public void setId(String id) {
    	this.id = id;
    }
    
    public long getMinTakenTime() {
    	return minTakenTime.getTime();
    }
    
    public long getMaxTakenTime() {
    	return maxTakenTime.getTime();
    }
    
    public long getTimeTakenRange() {
    	return items.isEmpty() ? -1 : (maxTakenTime.getTime() - minTakenTime.getTime());
    }
    
    public long getMedianTimeRange() {
    	long medianTimeTaken = getMedianTakenTime();
    	List<Long> diffs = new ArrayList<Long>();
    	for(Item item : items.values()) {
    		long diff = Math.abs(item.getTakenDate().getTime() - medianTimeTaken);
    		diffs.add(diff);
    	}
    	Collections.sort(diffs);
    	if(diffs.size() == 1)
			return diffs.get(0);
		
		Long lower = diffs.get(diffs.size()/2-1);
		Long upper = diffs.get(diffs.size()/2);
	 
		return (lower + upper) / 2;
    }
    
    public double getGeoRange() {
    	return eventGeoRange;
    }
    
    public double getMinRadious() {
    	Point medianGeo = getMedianGeo();
    	
    	if(medianGeo != null){
    		Double minDistance = Double.MAX_VALUE;
    		for(Item item : items.values()) {
    			if(item.hasLocation()){
    				Point p = new Point(item.getLongitude(), item.getLatitude());
    				Double distance = GeodesicDistanceCalculator.vincentyDistance(
						p.getLatitude(), p.getLongitude(), 
						medianGeo.getLatitude(), medianGeo.getLongitude());
    				if(minDistance > distance)
    					minDistance = distance;
    			}
    		}
    		return minDistance;
    	}
    	
    	return -1;
    }
    
    public double getMaxRadious() {
    	Point medianGeo = getMedianGeo();
    	
    	if(medianGeo!=null){
    		Double maxDistance = 0.0;
    		for(Item item : items.values()) {
    			if(item.hasLocation()){
    				Point p = new Point(item.getLongitude(), item.getLatitude());
    				Double distance = GeodesicDistanceCalculator.vincentyDistance(
						p.getLatitude(), p.getLongitude(), 
						medianGeo.getLatitude(), medianGeo.getLongitude());
    				if(maxDistance < distance)
    					maxDistance = distance;
    			}
    		}
    		return maxDistance;
    	}
    	return -1;
    }
    
    public double getMedianRadious() {
    	Point medianGeo = getMedianGeo();
    	if(medianGeo != null) {
    		List<Double> distances = new ArrayList<Double>();
    		for(Item item : items.values()) {
    			if(item.hasLocation()){
    				Point p = new Point(item.getLongitude(), item.getLatitude());
    				Double distance = GeodesicDistanceCalculator.vincentyDistance(
						p.getLatitude(), p.getLongitude(), 
						medianGeo.getLatitude(), medianGeo.getLongitude());
    				distances.add(distance);
    			}
    		}
    		Collections.sort(distances);
    		if(distances.isEmpty())
    			return -1;
    		if(distances.size()==1)
    			return distances.get(0);
    		
    		Double lower = distances.get(distances.size()/2-1);
    		Double upper = distances.get(distances.size()/2);
    	 
    		return (lower + upper) / 2;
    		
    	}
    	return -1;
    }
    
    public TFIDFVector getTitleVector() {
		return this.titleVector;
    }

    public TFIDFVector getTagsVector() {
		return this.tagsVector;
    }

    public TFIDFVector getDescriptionVector() {
		return this.descriptionVector;
    }

    public TFIDFVector getTextVector() {
    	TFIDFVector textVector = new TFIDFVector();
    	if(titleVector != null && !titleVector.isEmpty()) {
    		textVector.mergeVector(titleVector);
    	}
    	
    	if(tagsVector != null && !tagsVector.isEmpty()) {
    		textVector.mergeVector(tagsVector);
    	}
    	
    	if(descriptionVector != null && !descriptionVector.isEmpty()) {
    		textVector.mergeVector(descriptionVector);
    	}
    	
		return textVector;
    }
    
    public List<String> getTitleTerms() {
    	
    	if(titleVector != null && !titleVector.isEmpty()) {
    		List<String> terms = new ArrayList<String>();
    		for(Entry<String, TFIDF> termEntry : titleVector.entrySet()) {
    			int tf = (int) termEntry.getValue().tf;
				for(int i=0; i<tf; i++) {
					terms.add(termEntry.getKey());
				}
    		}
    		return terms;
    	}
    	
    	return null;
    }
    
    public List<String> getTagsTerms() {
    	
    	if(tagsVector != null && !tagsVector.isEmpty()) {
    		List<String> terms = new ArrayList<String>();
    		for(Entry<String, TFIDF> termEntry : tagsVector.entrySet()) {
    			int tf = (int) termEntry.getValue().tf;
				for(int i=0; i<tf; i++) {
					terms.add(termEntry.getKey());
				}
    		}
    		return terms;
    	}
    	
    	return null;
    }
    
    public List<String> getDescriptionTerms() {
    	
    	if(descriptionVector != null && !descriptionVector.isEmpty()) {
    		List<String> terms = new ArrayList<String>();
    		for(Entry<String, TFIDF> termEntry : descriptionVector.entrySet()) {
    			int tf = (int) termEntry.getValue().tf;
    			for(int i=0; i<tf; i++) {
    				terms.add(termEntry.getKey());
    			}
    		}
    		return terms;
    	}
    	
    	return null;
    }
    
    public String getTitle() {
    	String title = null;
    	double bestSim = .0;
    	TFIDFVector v1 = this.getTitleVector();
    	if(v1 != null) {
    		for(Item item : items.values()) {
    			TFIDFVector v2 = item.getTitleVector();
    			if(v2 == null)
    				continue;
    			
    			double similarity = v1.cosineSimilarity(v2);
    			if(bestSim < similarity) {
    				bestSim = similarity;
    				title = item.getTitle();
    			}
    		}
    	}
    	return title==null ? "No Title" : title;
    }
    
    public String getDescription() {
    	String description = null;
    	double bestSim = .0;
    	TFIDFVector v1 = this.getDescriptionVector();
    	if(v1 != null) {
    		for(Item item : items.values()) {
    			TFIDFVector v2 = item.getDescriptionVector();
    			if(v2 == null)
    				continue;
    			
    			double similarity = v1.cosineSimilarity(v2);
    			if(bestSim < similarity) {
    				bestSim = similarity;
    				description = item.getDescription();
    			}
    		}
    	}
    	return description==null ? "No Description" : description;
    }
    
    public Item getCentroidItem() {

    	Item bestItem = null;
    	double bestSim = .0;
    	TFIDFVector textVector = new TFIDFVector();
    	TFIDFVector titleVector = this.getTitleVector();
    	if(titleVector != null) {
    		textVector.mergeVector(titleVector);
    	}
    	TFIDFVector descVector = this.getDescriptionVector();
    	if(descVector != null) {
    		textVector.mergeVector(descVector);
    	}
    	TFIDFVector tagsVector = this.getTagsVector();
    	if(tagsVector != null) {
    		textVector.mergeVector(tagsVector);
    	}
    	
    	for(Item item : items.values()) {
    		TFIDFVector itemVector = item.getTextVector();
    		if(itemVector == null)
    			continue;
    			
    		double similarity = textVector.cosineSimilarity(itemVector);
    		if(bestSim < similarity) {
    			bestSim = similarity;
    			bestItem = item;
    		}
    	}
    	
    	return bestItem;
    }
    
    public boolean hasLocation() {
    	if(getMedianGeo()==null) {
    		return false;
    	}
    	return true;
    }
    
    public Point getMeanGeo() {
    	double latitude = 0, longitude = 0;
    	int geoLocatedItems = 0;
    	for(Item item : items.values()) {
    		if(item.hasLocation()) {
    			geoLocatedItems++;
    			latitude += item.getLatitude();
    			longitude += item.getLongitude();
    		}
    	}
    	
    	if(geoLocatedItems > 0) {
    		meanGeo = new Point(longitude/geoLocatedItems, latitude/geoLocatedItems);
    	}
    	
    	return meanGeo;
    }
    
    public Point getMedianGeo() {
    	medianGeo = null;
		Double minTotalDistance = Double.MAX_VALUE;
    	for(Item item1 : items.values()) {
    		if(!item1.hasLocation()) {
    			continue;
    		}
    		
    		Point p1 = new Point(item1.getLongitude(), item1.getLatitude());
    		Double totalDistance = 0.0;
			for(Item item2 : items.values()) {
    			if(item1 == item2)
    				continue;
    			
				if(!item2.hasLocation()) {
        			continue;
    			}
    			
    			Point p2 = new Point(item2.getLongitude(), item2.getLatitude());
    			Double distance = GeodesicDistanceCalculator.vincentyDistance(
    					p1.getLatitude(), p1.getLongitude(), 
    					p2.getLatitude(), p2.getLongitude());
    			
    			totalDistance += distance;
    			if(eventGeoRange < distance) {
    				eventGeoRange = distance;
    			}
        	}
			if(totalDistance < minTotalDistance) {
				medianGeo = p1;
				minTotalDistance = totalDistance;
    		}
    	}
    	return medianGeo;
    }
    
    public Point[] getPoints() {
    	
    	Collection<Point> points = new ArrayList<Point>();
    	for(Item item : items.values()) {
    		if(item.hasLocation()) {		
    			Point p = new Point(item.getLongitude(), item.getLatitude());
    			points.add(p);
    		}
    	}
    	return points.toArray(new Point[points.size()]);
    }
    
    
    public Point[] getPointsWithoutOutliers(double eps) {
    	
    	Collection<Point> points = new ArrayList<Point>();
    	for(Item item : items.values()) {
    		if(item.hasLocation()) {		
    			Point p = new Point(item.getLongitude(), item.getLatitude());
    			points.add(p);
    		}
    	}
    	
    	if(points.isEmpty()) {
    		return new Point[0];
    	}
    	
    	DBSCANClusterer<Point> clusterer = new DBSCANClusterer<Point>(eps, 2, new VincentyDistance());
    	List<Cluster<Point>> clusters = clusterer.cluster(points);
    	if(clusters.isEmpty()) {
    		return new Point[0];
    	}
    	
		Cluster<Point> maxCluster = Collections.max(clusters, new Comparator<Cluster<Point>>() {
			@Override
			public int compare(Cluster<Point> c1, Cluster<Point> c2) {
				return c2.getPoints().size() - c1.getPoints().size();
			}
		});
		
		if(maxCluster != null) {
			points = maxCluster.getPoints();
			return points.toArray(new Point[points.size()]);
		}
		else {
			return new Point[0];
		}
    }
    
	

	
    public long getMeanTakenTime() {
    	meanTakenTime = 0L;
    	for(Item item : items.values()) {
    		meanTakenTime += item.getTakenDate().getTime();
    	}
    	meanTakenTime /= items.size();
    	return meanTakenTime;
    }
    
    public long getMedianTakenTime() {
    	if(items.isEmpty()) {
    		return 0;
    	}
    	
    	if(items.size() == 1) {
    		Entry<String, Item> entry = items.entrySet().iterator().next();
    		Item item = entry.getValue();
    		medianTakenTime = item.getTakenDate().getTime();
    		
    		return medianTakenTime;
    	}
    	
    	List<Long> times = new ArrayList<Long>();
    	for(Item item : items.values()) {
    		times.add(item.getTakenDate().getTime());
    	}
    	Collections.sort(times);
    	
    	Long lower = times.get(times.size()/2-1);
    	Long upper = times.get(times.size()/2);
    	medianTakenTime = (lower + upper) / 2;
    
    	return medianTakenTime;
    }
    
    
    public String toString() {
    	return StringUtils.join(items.keySet(), ",");
    }

    public static List<Event> loadEventsFromFile(String eventsFile, Map<String, Item> items) {
    	List<Event> events = new ArrayList<Event>();
        List<String> lines;
		try {
			lines = IOUtils.readLines(new FileInputStream(eventsFile));
		} catch (Exception e) {
			e.printStackTrace();
			return events;
		} 
        for(String line : lines) {
        	Event event = new Event();
        	String[] ids = line.split(" ");
        	for(String id : ids) {
        		Item item = items.get(id);
        		event.addItem(item);
        	}
        	events.add(event);
        }
        return events;
    }
    
    public void merge(Event event) {
    	merges++;
    	mergedEvents.add(event.getId());
    	for(Item item : event.items.values()) {
    		this.addItem(item);
    	}
    	
    	GraphUtils.merge(eventGraph, event.getEventGraph());
	}
    
	public boolean isSameEvent(Event e, long timeDiff, double distanceDiff) {
		Point geo1 = getMeanGeo();
		Point geo2 = e.getMeanGeo();
    	if(geo1 == null || geo2 == null) {
    		return false;
    	}
    	
    	if(Math.abs(getMedianTakenTime()-e.getMedianTakenTime()) < timeDiff) {
    		double distance = GeodesicDistanceCalculator.vincentyDistance(
    				geo1.getLatitude(), geo1.getLongitude(), geo1.getLatitude(), geo1.getLongitude());
    		
    		if(distance < distanceDiff) {
    			return true;
    		}
    		
    	}
    	return false;
    }
	
	public boolean isSameEvent(Event e) {
		if(userSetSimilarity(e)>0.5) {
			return true;
		}
    	return false;
    }
	
	public void print() {
		StringBuffer strBfr = new StringBuffer();
		strBfr.append(this.getMinTakenTime() + ", ");
		strBfr.append(this.getMaxTakenTime() + ", ");
		strBfr.append(this.getMedianTakenTime() + ", ");
		strBfr.append(this.getTimeTakenRange() + ", ");
		strBfr.append(this.getMedianTimeRange() + ", ");
		strBfr.append(this.getMedianGeo() + ", ");
		strBfr.append(this.getMinRadious() + ", ");
		strBfr.append(this.getMaxRadious() + ", ");
		strBfr.append(this.getMedianRadious() + ", ");
		strBfr.append(this.getGeoRange());
		
		System.out.println(strBfr.toString());
	}

	public double locationSimilarity(Event event2) {
		Point geo1 = this.getMedianGeo();
		Point geo2 = event2.getMedianGeo();
		if(geo1!=null && geo2!=null) {
            return GeodesicDistanceCalculator.vincentyDistance(
            		geo1.getLatitude(), geo1.getLongitude(), geo2.getLatitude(), geo2.getLongitude());
		}
        else {
            return -1;
        }
	}

	public double userSetSimilarity(Event event2) {
		Set<String> intersection = new HashSet<String>(users);
		intersection.retainAll(event2.users);
		
		return intersection.size() / Math.min(users.size(), event2.users.size());
	}

	public double timeTakenSimilarity(Event event2) {
		
		long timeTaken1 = this.getMedianTakenTime();
		long timeTaken2 = event2.getMedianTakenTime();
    	
		double divisor = 1000.0 * 60 * 60;
        return Math.abs(timeTaken1-timeTaken2)/divisor;
    	
	}

	public double titleSimilarityBM25(Event event2) {
		TFIDFVector tdidfVector1 = this.getTitleVector();
		TFIDFVector tdidfVector2 = event2.getTitleVector();
		
		return tdidfVector1.BM25Similarity(tdidfVector2, 1);
	}

	public double titleSimilarityCosine(Event event2) {
		TFIDFVector tdidfVector1 = this.getTitleVector();
		TFIDFVector tdidfVector2 = event2.getTitleVector();
		
		return tdidfVector1.cosineSimilarity(tdidfVector2);
	}

	public double tagsSimilarityBM25(Event event2) {
		TFIDFVector tdidfVector1 = this.getTagsVector();
		TFIDFVector tdidfVector2 = event2.getTagsVector();
		
		return tdidfVector1.BM25Similarity(tdidfVector2, 1);
	}

	public double tagsSimilarityCosine(Event event2) {
		TFIDFVector tdidfVector1 = this.getTagsVector();
		TFIDFVector tdidfVector2 = event2.getTagsVector();
		
		return tdidfVector1.cosineSimilarity(tdidfVector2);
	}
	
	public double tagsOverlap(Event event2) {
		
		if(tags == null || tags.isEmpty() || 
				event2.tags == null || event2.tags.isEmpty()) {
			return .0;
		}
		
	    Set<String> intersection = new HashSet<String>();
	    Set<String> union = new HashSet<String>();
	    
		Set<String> tags1 = new HashSet<String>();
		for(String tag1 : tags.keySet()) {
			tags1.add(tag1.toLowerCase().trim());
		}
		Set<String> tags2 = new HashSet<String>();
		for(String tag2 : event2.tags.keySet()) {
			tags2.add(tag2.toLowerCase().trim());
		}
		
		intersection.addAll(tags1);
		intersection.retainAll(tags2);
		
		union.addAll(tags1);
		union.addAll(tags2);
		
		if(union.isEmpty()) {
			return .0;
		}
		
		return (double) intersection.size() / (double) union.size();
	}
	
	public double descriptionSimilarityBM25(Event event2) {
		TFIDFVector tdidfVector1 = this.getDescriptionVector();
		TFIDFVector tdidfVector2 = event2.getDescriptionVector();
		
		return tdidfVector1.BM25Similarity(tdidfVector2, 1);
	}

	public double descriptionSimilarityCosine(Event event2) {
		TFIDFVector tdidfVector1 = this.getDescriptionVector();
		TFIDFVector tdidfVector2 = event2.getDescriptionVector();
		
		return tdidfVector1.cosineSimilarity(tdidfVector2);
	}
	
	public double timeTakenDayDiff3(Event event2) {
		long timeTaken1 = this.getMedianTakenTime();
		long timeTaken2 = event2.getMedianTakenTime();
		
		double divisor = 1000.0;
        divisor = divisor * 60 * 60 * 24;
        double days = (timeTaken1 - timeTaken2) / divisor;
        if(days<3) {
            return 1.;
        }
        else {
            return 0.;
        }
	}

	public double timeTakenHourDiff12(Event event2) {
		long timeTaken1 = this.getMedianTakenTime();
		long timeTaken2 = event2.getMedianTakenTime();
		
		double divisor = 1000.0;
        divisor = divisor * 60 * 60;
        double hours = (timeTaken1 - timeTaken2) / divisor;
        if(hours < 12) {
            return 1.;
        }
        else { 
            return 0.;
        }
	}

	public double timeTakenHourDiff24(Event event2) {
		long timeTaken1 = this.getMedianTakenTime();
		long timeTaken2 = event2.getMedianTakenTime();
		
		double divisor = 1000.0;
        divisor = divisor * 60 * 60;
        double hours = (timeTaken1 - timeTaken2) / divisor;
        if(hours < 24) {
            return 1.;
        }
        else { 
            return 0.;
        }
	}
	
	public double overlap(Event other) {
	    Set<String> intersection = new HashSet<String>();
	   
		Set<String> ids1 = new HashSet<String>();
		for(String id : items.keySet()) {
			ids1.add(id);
		}
		Set<String> ids2 = new HashSet<String>();
		for(String id : other.items.keySet()) {
			ids2.add(id);
		}
		
		intersection.addAll(ids1);
		intersection.retainAll(ids2);
		
		return (double) intersection.size() / (double) Math.min(ids1.size(), ids2.size());
	}

	public Set<String> namedEntitiesOverlap(Event other) {
		
	    Set<String> intersection = new HashSet<String>();
	   
		Set<String> ne1 = new HashSet<String>();
		if(this.getPersons() != null) {
			ne1.addAll(this.getPersons().keySet());
		}
		if(this.getOrganizations() != null) {
			ne1.addAll(this.getOrganizations().keySet());
		}
		
		Set<String> ne2 = new HashSet<String>();
		if(other.getPersons() != null) {
			ne2.addAll(other.getPersons().keySet());
		}
		if(other.getOrganizations() != null) {
			ne2.addAll(other.getOrganizations().keySet());
		}
		
		intersection.addAll(ne1);
		intersection.retainAll(ne2);
		
		return intersection;
	}


	public DBObject toDBObject() {
		if(obj != null) {
			return obj;
		}
		
		obj = new BasicDBObject();
		obj.put("_id", id);
		obj.put("count", items.size());
		
		obj.put("endDate", maxTakenTime.getTime());
		obj.put("startDate", minTakenTime.getTime());
		
		obj.put("duration", (maxTakenTime.getTime()-minTakenTime.getTime()));
		
		List<String> itemIds = new ArrayList<String>();
		for(Item item : items.values()) {
			itemIds.add(item.getId());
		}
		obj.put("items", itemIds);
		
		obj.put("users", users);
		obj.put("numOfUsers", users.size());
		
		obj.put("merges", merges);
		if(mergedEvents == null || mergedEvents.isEmpty()) {
			obj.put("mergedEvents", new ArrayList<String>());
		}
		else {
			obj.put("mergedEvents", new ArrayList<String>(mergedEvents));
		}
		
		obj.put("density", getDensity());
		
		obj.put("title", getTitle());
		obj.put("description", getDescription());
		
		Item item = getCentroidItem();
		if(item != null) {
			obj.put("url", item.getUrl());
		}
		
		Map<Long, Collection<String>> histogram = timeline.getHistogram();
		BasicDBList timeline = new BasicDBList();
		BasicDBList itemsTimeline = new BasicDBList();
		for(Entry<Long, Collection<String>> hEntry : histogram.entrySet()) {
			DBObject binCount = new BasicDBObject();
			binCount.put("bin", hEntry.getKey());
			Collection<String> binItems = hEntry.getValue();
			binCount.put("count", binItems.size());
			timeline.add(binCount);
			
			DBObject binSet = new BasicDBObject();
			binSet.put("bin", hEntry.getKey());
			binSet.put("items", hEntry.getValue());
			itemsTimeline.add(binSet);
		}
		obj.put("timeline", timeline);
		obj.put("itemsTimeline", itemsTimeline);
		
		if(titleVector != null) {
			obj.put("titleVector", titleVector.getTFMap());
		}
		
		if(tagsVector != null) {
			obj.put("tagsVector", tagsVector.getTFMap());
		}

		if(descriptionVector != null) {
			obj.put("descriptionVector", descriptionVector.getTFMap());
		}
		
		if(tags != null && !tags.isEmpty()) {
			List<DBObject> tagsMap = new ArrayList<DBObject>();
			for(Entry<String, Integer> tagEntry : tags.entrySet()) {
				DBObject obj = new BasicDBObject();
				obj.put("tag", tagEntry.getKey());
				obj.put("count", tagEntry.getValue());
				
				tagsMap.add(obj);
			}
			obj.put("tags", tagsMap);
		}
		
		if(machineTags != null && !machineTags.isEmpty()) {
			obj.put("machineTags", new ArrayList<String>(machineTags));
		}
		
		if(persons != null && !persons.isEmpty()) {
			List<DBObject> personsMap = new ArrayList<DBObject>();
			for(Entry<String, Integer> personEntry : persons.entrySet()) {
				DBObject obj = new BasicDBObject();
				obj.put("person", personEntry.getKey());
				obj.put("count", personEntry.getValue());
				
				personsMap.add(obj);
			}
			obj.put("persons", personsMap);
		}
		if(locations != null && !locations.isEmpty()) {
			List<DBObject> locationsMap = new ArrayList<DBObject>();
			for(Entry<String, Integer> locationEntry : locations.entrySet()) {
				DBObject obj = new BasicDBObject();
				obj.put("location", locationEntry.getKey());
				obj.put("count", locationEntry.getValue());
				
				locationsMap.add(obj);
			}
			obj.put("locations", locationsMap);
		}
		if(organizations != null && !organizations.isEmpty()) {
			List<DBObject> organizationsMap = new ArrayList<DBObject>();
			for(Entry<String, Integer> organizationEntry : organizations.entrySet()) {
				DBObject obj = new BasicDBObject();
				obj.put("organization", organizationEntry.getKey());
				obj.put("count", organizationEntry.getValue());
				
				organizationsMap.add(obj);
			}
			obj.put("organizations", organizationsMap);
		}
		
		Point p = getMeanGeo();
		if(p != null) {
			DBObject location = new BasicDBObject();
			
			Double[] coordinates = new Double[2];
			coordinates[0] = p.getLatitude();
			coordinates[1] = p.getLongitude();
			location.put("coordinates", coordinates);
			
			Point[] points = getPoints();
			if(points != null && points.length > 0) {
				int k = 0;
				double maxRadius = .0, avgRadius = .0;
				for(int i=0; i<points.length; i++) {
					for(int j=i+1; j<points.length; j++) {
						Point p1 = points[i];
						Point p2 = points[j];
						Double distance = GeodesicDistanceCalculator.vincentyDistance(p1, p2);
						if(distance != null) {
							if(distance > maxRadius) {
								maxRadius = distance;
							}
							k++;
							avgRadius += distance;
						}
					}
				}
				
				avgRadius = (k == 0) ? .0 : (avgRadius / (double) k);
				
				location.put("maxRadius", maxRadius);
				location.put("avgRadius", avgRadius);
				
				double radiusInKm = avgRadius/1000;
				Point[] pointsWithoutOutliers = null;
				if(radiusInKm < 100) {
					obj.put("locationType", "local");
					obj.put("location", location);
					obj.put("hasLocation", true);
					
					pointsWithoutOutliers = getPointsWithoutOutliers(100000);
				}
				else if(radiusInKm > 100 && radiusInKm < 1000) {
					obj.put("locationType", "regional");
					obj.put("location", location);
					obj.put("hasLocation", true);
					
					pointsWithoutOutliers = getPointsWithoutOutliers(500000);
				}
				else {
					obj.put("locationType", "global");
					obj.put("hasLocation", false);
				}
				
				if(pointsWithoutOutliers != null && pointsWithoutOutliers.length > 0) {
					ExtGrahamScan gScan = new ExtGrahamScan(pointsWithoutOutliers);
					List<Map<String, Double>> convexHull = gScan.convexHull();
					location.put("convexHull", convexHull);
				}
				
			}
			else {
				obj.put("locationType", "non");
				obj.put("hasLocation", false);
			}
		}
		else {
			obj.put("hasLocation", false);
		}
		
		obj.put("scores", this.getEventScores());
		
		return obj;
	}

	public SolrInputDocument toSolrInputDocument() {
		DBObject dbObject = this.toDBObject();
		
		SolrInputDocument doc = new SolrInputDocument();
		
		doc.setField("id", dbObject.get("_id"));
		
		doc.setField("count", dbObject.get("count"));
		
		doc.setField("endDate", dbObject.get("endDate"));
		doc.setField("startDate", dbObject.get("startDate"));
		
		doc.setField("duration", dbObject.get("duration"));
		doc.setField("merges", dbObject.get("merges"));
		doc.setField("mergedEvents", dbObject.get("mergedEvents"));
		doc.setField("numOfUsers", dbObject.get("numOfUsers"));
		
		doc.setField("url", dbObject.get("url"));
		
		doc.setField("title", dbObject.get("title"));
		doc.setField("description", dbObject.get("description"));
		
		List<String> titleTerms = getTitleTerms();
		if(titleTerms != null && !titleTerms.isEmpty()) {
			doc.setField("titleTerms", titleTerms);
		}
		
		List<String> descriptionTerms = getDescriptionTerms();
		if(descriptionTerms != null && !descriptionTerms.isEmpty()) {
			doc.setField("descriptionTerms", descriptionTerms);
		}
		
		doc.setField("density", dbObject.get("density"));
		
		if(this.tags != null && !this.tags.isEmpty()) {
			doc.setField("tags", new ArrayList<String>(tags.keySet()));
		}
		
		doc.setField("machineTags", dbObject.get("machineTags"));
		
		if(this.persons != null && !this.persons.isEmpty()) {
			doc.setField("persons", new ArrayList<String>(persons.keySet()));
		}
		if(this.locations != null && !this.locations.isEmpty()) {
			doc.setField("locations", new ArrayList<String>(locations.keySet()));
		}
		if(this.organizations != null && !this.organizations.isEmpty()) {
			doc.setField("organizations", new ArrayList<String>(organizations.keySet()));
		}
		
		String locationType = (String) dbObject.get("locationType");
		doc.setField("locationType", locationType);
		
		boolean hasLocation = (boolean) dbObject.get("hasLocation");
		doc.setField("hasLocation", hasLocation);
		
		
		DBObject location = (DBObject) dbObject.get("location");
		if(location != null) {
			Double[] coordinates = (Double[]) location.get("coordinates");
			doc.setField("latlon", StringUtils.join(coordinates, ","));
			
			List<String> convexHullList = new ArrayList<String>();
			@SuppressWarnings("unchecked")
			List<Map<String, Double>> convexHull = (List<Map<String, Double>>) location.get("convexHull");
			if(convexHull != null) {
				for(Map<String, Double> point : convexHull) {
					Double latitude = point.get("latitude");
					Double longitude = point.get("longitude");
					
					String latlon = latitude + "," + longitude;
					convexHullList.add(latlon);
				}
			}

			if(!convexHullList.isEmpty()) {
				doc.setField("convexHull", convexHullList);
			}
		}
		
		return doc;
	}
	
	public int size() {
		return items.size();
	}

	public Map<String, Item> getItemsMap() {
		return items;
	}
	
	public ItemsTimeline getTimeline() {
		return timeline;
	}

	public void setTimeline(ItemsTimeline timeline) {
		this.timeline = timeline;
	}

	public int getMerges() {
		return merges;
	}

	public void setMerges(int merges) {
		this.merges = merges;
	}

	public Set<String> getMergedEvents() {
		return mergedEvents;
	}

	public void setMergedEvents(Set<String> mergedEvents) {
		this.mergedEvents = mergedEvents;
	}
	
	public Map<String, Integer> getPersons() {
		return persons;
	}

	public void setPersons(Map<String, Integer> persons) {
		this.persons = persons;
	}

	public Map<String, Integer> getLocations() {
		return locations;
	}

	public void setLocations(Map<String, Integer> locations) {
		this.locations = locations;
	}

	public Map<String, Integer> getOrganizations() {
		return organizations;
	}

	public void setOrganizations(Map<String, Integer> organizations) {
		this.organizations = organizations;
	}
	
	public Graph<String, SameEventLink> getEventGraph() {
		return eventGraph;
	}

	public void setEventGraph(Graph<String, SameEventLink> graph) {
		this.eventGraph = graph;
	}

	public Map<String, Double> getEventScores() {
		return eventScores;
	}

	public void setEventScores(Map<String, Double> eventScores) {
		this.eventScores = eventScores;
	}

	public void updateTextualContent() {
		getTitleVector().computeLength();
		getDescriptionVector().computeLength();
		getTagsVector().computeLength();
		getTextVector().computeLength();
	}

	public double getDensity() {
		return density;
	}

	public void setDensity(double density) {
		this.density = density;
	}

	public Set<String> getMachineTags() {
		return machineTags;
	}

	public void setMachineTags(Set<String> machineTags) {
		this.machineTags = machineTags;
	}
	
}