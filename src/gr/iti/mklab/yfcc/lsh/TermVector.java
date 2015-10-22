package gr.iti.mklab.yfcc.lsh;

import gr.iti.mklab.yfcc.models.TFIDF;
import gr.iti.mklab.yfcc.models.TFIDFVector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.List;


public class TermVector implements Iterable<Entry<Integer, Double>> {
	
	static int c = 0;

	String id;
	TFIDFVector v;
	List<String> terms = new ArrayList<String>();

	private Map<Integer, Double> features;
	
	public TermVector(TFIDFVector v) { 
		this.v = v;
		terms.addAll(v.keySet());
	}


	public int tf(String term) {
		TFIDF tfidf = v.get(term);
		if(tfidf == null) {
			return 0;
		}
		
		return (int) tfidf.tf;
	}
	
	public double idf(String term) {
		TFIDF tfidf = v.get(term);
		if(tfidf == null) {
			return .0;
		}
		return tfidf.idf;
	}
	
	public int index(String term) {
		return terms.indexOf(term);
	}
	
	public String[] tokens() {
		return v.keySet().toArray(new String[v.size()]);
	}
	
	public String id(){
		return id;
	}

	@Override
	public Iterator<Entry<Integer, Double>> iterator() {
		if(features != null) {
			return features.entrySet().iterator();
		}
		
		features = new HashMap<Integer, Double>();
		for(String term : terms) {
			int index = index(term);
			double w = tf(term) * idf(term);
			features.put(index, w);
		}
		return features.entrySet().iterator();
	}

	
	public double cosineSimilarity(TermVector candidate) {
		
		Iterator<Entry<Integer, Double>> it1 = this.iterator();
		double magnitude1 = 0;
		while(it1.hasNext()) {
			magnitude1 += Math.pow(it1.next().getValue(), 2);
		}
		Iterator<Entry<Integer, Double>> it2 = candidate.iterator();
		double magnitude2 = 0;
		while(it2.hasNext()) {
			magnitude2 += Math.pow(it2.next().getValue(), 2);
		}
		
		double denominator = Math.sqrt(magnitude1 * magnitude2);
		if(denominator<0.000000000000001)
			return 0.0;
		
		double numerator = 0.0;
		double w1=-1, w2=-1;
		Set<String> both = new HashSet<String>(terms);
        both.retainAll(candidate.terms);
		for(String term : both) {
			w1 = this.tf(term) * this.idf(term); 
			w2 = candidate.tf(term) * candidate.idf(term); 
			numerator += w1 * w2;
		}

		return numerator / denominator;
	}

}
