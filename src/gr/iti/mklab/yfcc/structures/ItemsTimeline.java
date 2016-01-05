package gr.iti.mklab.yfcc.structures;

import gr.iti.mklab.yfcc.models.Item;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;

public class ItemsTimeline implements Serializable, Iterable<Entry<Long, Collection<String>>> {

	private static final long serialVersionUID = -6107744748512784691L;
	
	protected TreeMap<Long, Collection<String>> histogram = new TreeMap<Long, Collection<String>>();
	
	protected Set<Long> minimaBins = new HashSet<Long>();
	
	protected long div;
	protected Long minTime = Long.MAX_VALUE, maxTime = Long.MIN_VALUE;
	
	public ItemsTimeline(int time, TimeUnit tu) {
		div = TimeUnit.MILLISECONDS.convert(time, tu);
	}
	
	private Integer total = 0;
	
	public void put(String id, Date date) {
		
		total += 1; 
		
		Long key = date.getTime();
		key = (key/div)*div;
		
		if(key > maxTime) {
			maxTime = key;
		}
		
		if(key < minTime) {
			minTime = key;
		}
		
		Collection<String> timeslot = histogram.get(key);
		if(timeslot == null) {
			timeslot = new HashSet<String>();
			histogram.put(key, timeslot);
		}
		timeslot.add(id);
	}
	
	public Integer getFrequency(Long key) {
		key = (key/div)*div;
		Collection<String> timeslot = histogram.get(key);
		if(timeslot == null) {
			return 0;
		}
		return timeslot.size();
	}
	
	public Integer getFrequency(Pair<Long, Long> window) {
		Integer freq = 0;
		
		long t1 = (window.getLeft() / div) * div;
		long t2 = (window.getRight() / div) * div;
		
		for(long t = t1; t <= t2; t += div) {
			freq += getFrequency(t);
		}
		return freq;
	}
	
	public Integer getTotal() {
		return total;
	}
	
	public void serialize(String filename) throws IOException {
		FileOutputStream fileOut = new FileOutputStream(filename);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		
		out.writeObject(this);
        out.close();
        fileOut.close();
	}
	
	public static ItemsTimeline deserialize(String filename) throws IOException, ClassNotFoundException {
		FileInputStream fileIn = new FileInputStream(filename);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        ItemsTimeline obj = (ItemsTimeline) in.readObject();
        in.close();
        fileIn.close();
        
        return obj;
	}
	
	public Long getTimeslotLength() {
		return div;
	}
	
	public Map<Long, Collection<String>> getHistogram() {
		return this.histogram;
	}
	
	public int size() {
		return histogram.size();
	}
	
	public static ItemsTimeline createTimeline(int time, TimeUnit tu, Collection<Item> items) {
		ItemsTimeline timeline = new ItemsTimeline(time, tu);
		for(Item item : items) {
			timeline.put(item.getId(), item.getTakenDate());
		}
		return timeline;
	}
	
	public static ItemsTimeline createTimeline(int time, TimeUnit tu, Iterator<Item> it) {
		ItemsTimeline timeline = new ItemsTimeline(time, tu);
		while(it.hasNext()) {
			Item item = it.next();
			timeline.put(item.getId(), item.getTakenDate());
		}
		return timeline;
	}
	
	public void merge(ItemsTimeline other) {
		for(Entry<Long, Collection<String>> entry : other.histogram.entrySet()) {
			Long key = entry.getKey();
			Collection<String> timeslost = entry.getValue();
			
			Collection<String> thisTimeslot = histogram.get(key);
			if(thisTimeslot != null) {
				total -= thisTimeslot.size();
				thisTimeslot.addAll(timeslost);
				total += thisTimeslot.size();
			}
			else {
				histogram.put(key, timeslost);
				total += timeslost.size();
			}
		}
	}
	
	public Long getMinTime() {
		return Collections.min(histogram.keySet());
	}
	
	public Long getMaxTime() {
		return Collections.max(histogram.keySet());
	}
	
	public long getMean() {
    	long meanTakenTime = 0L;
    	for(Entry<Long, Collection<String>> entry : histogram.entrySet()) {
    		meanTakenTime += (entry.getKey() * entry.getValue().size());
    	}
    	if(total != null) {
    		meanTakenTime /= total;
    	}
    	
    	return meanTakenTime;
    }
    
    public long getMedian() {
    	long medianTakenTime = 0;
    	if(histogram.isEmpty()) {
    		return medianTakenTime;
    	}
    	
    	List<Long> times = new ArrayList<Long>();
    	for(Entry<Long, Collection<String>> entry : histogram.entrySet()) {
    		for(int i=0; i<entry.getValue().size(); i++) {
    			times.add(entry.getKey());
    		}
    	}
    	
    	if(times.size() == 1) {
    		return times.get(0);
    	}
    	
    	Collections.sort(times);
    	
    	Long lower = times.get(times.size()/2-1);
    	Long upper = times.get(times.size()/2);
    	medianTakenTime = (lower + upper) / 2;
    
    	return medianTakenTime;
    }
    
	@Override
	public Iterator<Entry<Long, Collection<String>>> iterator() {
		return histogram.entrySet().iterator();
	}
	
	public Set<Entry<Long, Collection<String>>> entrySet() {
		return histogram.entrySet();
	}
}
