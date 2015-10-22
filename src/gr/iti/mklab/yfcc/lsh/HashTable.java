package gr.iti.mklab.yfcc.lsh;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HashTable extends HashMap<Signature, List<Object>> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 223843523508594017L;
	private HashFamily hashFamily = null;
 
	public HashTable(int k, int d) {
		
		hashFamily = new HashFamily(k, d);
	}
	
	public void add(TermVector tv) {
		Signature hash = hashFamily.hr(tv);
		//System.out.print(hash.hashCode()+"   ");
		//System.out.println(vsm.text());
		if(this.containsKey(hash)) {
			 List<Object> bucket = this.get(hash);
			 bucket.add(tv);
		}
		else{
			List<Object> bucket = new ArrayList<Object>();
			bucket.add(tv);
			this.put(hash, bucket);
		}
	}

	public List<Object> get(TermVector tv) {
		Signature hash = hashFamily.hr(tv);
		List<Object> bucket = this.get(hash);
		if(bucket != null)
			return bucket;
		else
			return new ArrayList<Object>();
	}

	public void addUniqueVector(TermVector tv) {
		Signature hash = hashFamily.hr(tv);
		if(this.containsKey(hash)) {
			 List<Object> bucket = this.get(hash);
			 for(Object other : bucket) {
				 double similarity = tv.cosineSimilarity((TermVector) other);
				 if(similarity>0.8) {
					return;
				 }
			 }
			bucket.add(tv);
		}
		else {
			List<Object> bucket = new ArrayList<Object>();
			bucket.add(tv);
			this.put(hash, bucket);
		}
		
	}
}
