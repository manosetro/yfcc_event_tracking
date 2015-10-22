package gr.iti.mklab.yfcc.lsh;

import gr.iti.mklab.yfcc.models.RankedObject;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.aliasi.util.BoundedPriorityQueue;

public class HashTables {

	HashTable hashTables[];
	
	public HashTables(int L, int k, int d) {
		hashTables = new HashTable[L];
		for(int i=0;i<L;i++) {
			hashTables[i] = new HashTable(k,d);
		}
	}
	
	public void add(TermVector tv) {
		for(HashTable hashTable : hashTables) {
			hashTable.add(tv);
		}
	}
	
	public void addUniqueVector(TermVector tv) {
		for(HashTable hashTable : hashTables) {
			hashTable.addUniqueVector(tv);
		}
	}
	
	public TermVector[] get(TermVector tv) {
		Set<Object> set = new HashSet<Object>();
		for(HashTable hashTable : hashTables) {
			List<Object> bucket = hashTable.get(tv);
			if(bucket!=null) {
				set.addAll(bucket);
			}
		}
		return set.toArray(new TermVector[set.size()]);
	}
	
	public RankedObject[] getNN(TermVector tv, int N) {
		BoundedPriorityQueue<RankedObject> nn = 
				new BoundedPriorityQueue<RankedObject>(new RankedObject(), N);
		TermVector[] condidate_nns = get(tv);
		for(TermVector candidate : condidate_nns){
			double similarity = tv.cosineSimilarity(candidate);
			nn.offer(new RankedObject(candidate.id(), similarity));
		}
		
		return nn.toArray(new RankedObject[nn.size()]);
	}
	
	public RankedObject getNearest(TermVector tv) {
		BoundedPriorityQueue<RankedObject> nn = 
				new BoundedPriorityQueue<RankedObject>(new RankedObject(), 1);
		TermVector[] condidate_nns = get(tv);
		for(TermVector candidate : condidate_nns){
			double similarity = tv.cosineSimilarity(candidate);
			nn.offer(new RankedObject(candidate.id(), similarity));
		}
		return nn.peek();
	}
	
	public void clear() {
		for(HashTable hashTable : hashTables) {
			hashTable.clear();
		}
	}
	
}
