package gr.iti.mklab.yfcc.structures;

import gr.iti.mklab.yfcc.models.Item;
import gr.iti.mklab.yfcc.models.TFIDFVector;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;


public class ExtendedTimeline extends Timeline {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6107744748512784691L;
	
	private TreeMap<Long, TFIDFVector> vectorsMap = new TreeMap<Long, TFIDFVector>();
	private TreeMap<Long, List<String>> items = new TreeMap<Long, List<String>>();
	
	private Long minTime = Long.MAX_VALUE, maxTime = Long.MIN_VALUE;
	
	public ExtendedTimeline(int time, TimeUnit tu) {
		super(time, tu);
	}

	public TFIDFVector put(Long key, TFIDFVector vector, String id) {
		
		super.put(key, 1);
		
		key = (key/div) * div;
		if(key > maxTime) {
			maxTime = key;
		}
		if(key < minTime) {
			minTime = key;
		}
		
		List<String> binItems = items.get(key);
		if(binItems == null) {
			binItems = new ArrayList<String>();
			items.put(key, binItems);
		}
		binItems.add(id);
		
		TFIDFVector currValue = vectorsMap.get(key);
		if(currValue == null) {
			return vectorsMap.put(key, vector);
		}
		else {
			currValue.mergeVector(vector);
			return vectorsMap.put(key, currValue);
		}
	}

	public TFIDFVector getVector(Long key) {
		TFIDFVector vector = vectorsMap.get(key);
		return vector;
	}
	
	public TFIDFVector getVector(Pair<Long, Long> window) {
		TFIDFVector mergedVector = new TFIDFVector();
		
		long t1 = (window.getLeft() / div) * div;
		long t2 = (window.getRight() / div) * div;
		for(long t = t1; t <= t2; t += div) {
			TFIDFVector vector = getVector(t);
			if(vector != null) {
				mergedVector.mergeVector(vector);
			}
		}
		return mergedVector;
	}
	
	public List<String> getItems(Long key) {
		
		key = (key / div) * div;
		
		List<String> binItems = items.get(key);
		if(binItems == null) {
			return new ArrayList<String>();
		}
		
		return binItems;
	}
	
	public List<String> getItems(Pair<Long, Long> window) {
		List<String> allItems = new ArrayList<String>();
		
		long t1 = (window.getLeft() / div) * div;
		long t2 = (window.getRight() / div) * div;		
		for(long t = t1; t <= t2; t += div) {
			List<String> itemIds = getItems(t);
			if(itemIds != null) {
				allItems.addAll(itemIds);
			}
		}
		return allItems;
	}
	
	public static ExtendedTimeline deserialize(String filename) throws IOException, ClassNotFoundException {
		FileInputStream fileIn = new FileInputStream(filename);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        ExtendedTimeline obj = (ExtendedTimeline) in.readObject();
        in.close();
        fileIn.close();
        
        return obj;
	}
	
	public void merge(ExtendedTimeline other) {
		super.merge(other);
		
		Set<Long> keys = new HashSet<Long>();
		keys.addAll(other.vectorsMap.keySet());
		keys.addAll(this.vectorsMap.keySet());
		keys.addAll(other.items.keySet());
		keys.addAll(this.items.keySet());
		
		for(Long key : keys) {
			TFIDFVector otherVector = other.vectorsMap.get(key);
			TFIDFVector thisVector = this.getVector(key);
			if(thisVector == null) {
				thisVector = new TFIDFVector();
			}
			if(otherVector != null) {
				thisVector.mergeVector(otherVector);
			}
			
			this.vectorsMap.put(key, thisVector);

			List<String> otherMessages = other.items.get(key);
			List<String> thisMessages = this.getItems(key);
			
			if(thisMessages == null) {
				thisMessages = new ArrayList<String>();
			}
			if(otherMessages != null) {
				thisMessages.addAll(otherMessages);
			}
			
			
			// Keep unique instances
			Set<String> hs = new HashSet<String>();
			hs.addAll(thisMessages);
			thisMessages.clear();
			thisMessages.addAll(hs);
			
			this.items.put(key, thisMessages);
		}
		
		this.minTime = Math.min(this.minTime, other.minTime);
		this.maxTime = Math.max(this.maxTime, other.maxTime);
		
	}
	
	public static ExtendedTimeline createTimeline(int time, TimeUnit tu,Map<String, Pair<TFIDFVector, Long>> vectors) {
		ExtendedTimeline timeline = new ExtendedTimeline(time, tu);
		for(Entry<String, Pair<TFIDFVector, Long>> e : vectors.entrySet()) {
			String id = e.getKey();
			
			Pair<TFIDFVector, Long> value = e.getValue();
			Long publicationTime = value.getRight();
			TFIDFVector vector = value.getLeft();
			if(vector == null || publicationTime == null)
				continue;
			
			timeline.put(publicationTime, vector, id);
		}
		return timeline;
	}
	
	public static ExtendedTimeline createTimeline(int time, TimeUnit tu, Map<String, TFIDFVector> vectors, Collection<Item> items) {
		ExtendedTimeline timeline = new ExtendedTimeline(time, tu);
		for(Item item : items) {
			
			String id = item.getId();
			Date takenDate = item.getTakenDate();
			
			TFIDFVector vector = vectors.get(id);
			if(vector == null || takenDate == null) {
				continue;
			}
			
			timeline.put(takenDate.getTime(), vector, id);
		}
		return timeline;
	}
	
}
