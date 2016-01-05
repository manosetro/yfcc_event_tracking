package gr.iti.mklab.yfcc.vindex;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 
 * @author Manos Schinas (manosetro@iti.gr)
 *
 */
@XmlRootElement
public class Vector {
	
	@XmlElement 
	public String id;
	
	@XmlElement 
	public double[] vector;
	
	@XmlElement 
	public int dimension;
	
	public Vector() {
		
	}
	
	public Vector(String id, double[] vector) {
		this.id = id;
		
		
		this.vector = vector;
		this.dimension = (vector == null) ? 0 : vector.length;
	}
	
	@Override
	public boolean equals(Object o) {
		Vector oV = (Vector) o;
		return id.equals(oV.id);
	}
	
	@Override
	public int hashCode() {
		return id.hashCode();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public double[] getVector() {
		return vector;
	}

	public void setVector(double[] vector) {
		this.vector = vector;
	}
	
	public int getDimension() {
		return dimension;
	}

	public void setDimension(int dimension) {
		this.dimension = dimension;
	}
}