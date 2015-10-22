package gr.iti.mklab.yfcc.models;

import java.util.*;

public class TFIDFVector extends HashMap<String, TFIDF> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

    //Some parameters that are used for BM25 computation
    private static double BM25_K1 = 1.5;
    private static double BM25_B = 0.75;
    
    double vectorLength = 0;
    
    public TFIDFVector() {
    	super();
    }

    public void addTerm(String term, double tf, double idf) {
    	
    	TFIDF f = new TFIDF();
    	f.tf = tf;
    	f.idf = idf;
    	f.tfidf = tf * idf;
    	
    	put(term, f);
    }
    
    public void addTerm(String term, TFIDF f) {
    	put(term, f);
    }
    
    public void update(String term, double tf, double idf) {
    	
    	TFIDF tfIdf = get(term);
    	if(tfIdf == null) {
    		addTerm(term, tf, idf);
    	}
    	else {
    		tfIdf.tf = tfIdf.tf + tf;
    		tfIdf.tfidf = tfIdf.tf * idf;
    	}
    	
    }
 
    public void mergeVector(TFIDFVector other) {
		for(Entry<String, TFIDF> entry : other.entrySet()) {
			
			String term = entry.getKey();
			
			TFIDF newTfIdf = this.get(term);
			if(newTfIdf == null) {
				newTfIdf = new TFIDF();	 
				TFIDF oldTfIdf = entry.getValue();
			
				newTfIdf.tf = oldTfIdf.tf;
				newTfIdf.idf = oldTfIdf.idf;
				newTfIdf.tfidf = oldTfIdf.tfidf;
			}
			else {
				TFIDF oldTfIdf = entry.getValue();
				newTfIdf.tf = newTfIdf.tf + oldTfIdf.tf;
			}
			this.put(term, newTfIdf);
		}
	}
     
    public void computeLength() {
        vectorLength = 0;
        Collection<TFIDF> freqs = values();
        for(TFIDF freq : freqs) {
            vectorLength += Math.pow(freq.tfidf, 2);
        }
        
        vectorLength = Math.sqrt(vectorLength);
    }

    public double getLength() {
    	return vectorLength;
    }
    
    public double computeLengthSubset(Set<String> subset) {
        double vectorLength = .0;
        
        Set<String> words = keySet();
        Set<String> intersection = new HashSet<String>(subset);
        intersection.retainAll(words);
        for(String next_word : intersection) {
        	TFIDF freq = get(next_word);
            vectorLength += (freq.tfidf * freq.tfidf);
        }
        
        return vectorLength = Math.sqrt(vectorLength);
    }
    
    
    public double cosineSimilarity(TFIDFVector vector2) {
    	
        if(vectorLength == 0) 
        	computeLength();
        
        if(vectorLength == 0) 
        	return .0;
        
        if(vector2.vectorLength == 0) 
        	vector2.computeLength();
        
        if(vector2.vectorLength == 0) 
        	return .0;
        
        
        
        // Find intersection
        Set<String> words1 = keySet();
        Set<String> words2 = vector2.keySet();
        Set<String> intersection = new HashSet<String>(words1);
        intersection.retainAll(words2);
        
        double similarity = .0;
        for(String matchedTerm : intersection) {
            TFIDF fre1 = this.get(matchedTerm);
            TFIDF fre2 = vector2.get(matchedTerm);
            similarity += (fre1.tfidf * fre2.tfidf);
        }
        similarity = similarity / (vectorLength * vector2.vectorLength);
        
        return similarity;
    }

    public double cosineSimilaritySubset(TFIDFVector vector2, Set<String> subset) {
        
    	double vectorLength1 = computeLengthSubset(subset);
        double vectorLength2 = vector2.computeLengthSubset(subset);
        
        if(vectorLength1 == 0) 
        	return 0;
        
        if(vectorLength2 == 0) 
        	return 0;

        double similarity = 0;
        
        Set<String> words1 = this.keySet();
        Set<String> words2 = vector2.keySet();
        Set<String> intersection = new HashSet<String>(subset);
        intersection.retainAll(words1);
        intersection.retainAll(words2);

        for(String match : intersection) {
        	TFIDF fre1 = this.get(match);
        	TFIDF fre2 = vector2.get(match);
            similarity += (fre1.tfidf * fre2.tfidf);
        }
        similarity = similarity / (vectorLength1*vectorLength2);
        return similarity;
    }
    
    public double BM25Similarity(TFIDFVector vector2, double avgLength) {
    	
        double similarity1 = 0, similarity2 = 0;
        double d1 = 0, d2 = 0;
        
        for(TFIDF freq : values()) {
            d1 += freq.tf;
        }
        
        for(TFIDF freq:vector2.values()) {
            d2 += freq.tf;
        }
        
        Set<String> words1 = keySet();
        Set<String> words2 = vector2.keySet();
        Set<String> intersection = new HashSet<String>(words1);
        intersection.retainAll(words2);
        
        for(String match : intersection) {
        	TFIDF fre1 = get(match);
            similarity1 += fre1.idf * (fre1.tf * (BM25_K1+1))/(fre1.tf + BM25_K1 * (1 - BM25_B + BM25_B * (d1/avgLength)));       
        }

        for(String match:intersection) {
        	TFIDF fre2 = vector2.get(match);
            similarity2 += fre2.idf * (fre2.tf * (BM25_K1 + 1))/(fre2.tf + BM25_K1 * (1 - BM25_B + BM25_B * (d2/avgLength)));
        }
        
        return (similarity1 + similarity2)/2;
    }
    
	public Map<String, Integer> getTFMap() {
		Map<String, Integer> tfMap = new HashMap<String, Integer>();
		for(Entry<String, TFIDF> entry : this.entrySet()) {
			tfMap.put(entry.getKey(), (int) entry.getValue().tf);
		}
		return tfMap;
	}
	
}
