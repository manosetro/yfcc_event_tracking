package gr.iti.mklab.yfcc.models;

import java.io.Serializable;


public class SameEventLink implements Serializable, Comparable<SameEventLink> {
    
	/**
	 * 
	 */
	private static final long serialVersionUID = 7977760397342134179L;
	
	public Double weight;
    //public String id;
    public static int counter;
    
    public SameEventLink() {
    }

    public SameEventLink(double weight) {
        this.weight = weight;
        counter += 1;
    }

	public String toString() {
		return weight.toString();
	}
	
	@Override
	public int compareTo(SameEventLink that) {
		if (this.weight <= that.weight) {
	    	return -1;
		}
	    else { 
	    	return 1;
	    }
	}
    
}