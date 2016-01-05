package gr.iti.mklab.yfcc.vindex;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 
 * @author Manos Schinas (manosetro@iti.gr)
 *
 */
@XmlRootElement
public class NearestNeighbors {
	
	@XmlElement 
	public String id;
	
	@XmlElement 
	public String url;
	
	@XmlElement 
	public List<NearestNeighbor> nearest = new ArrayList<NearestNeighbor>();
	
	public NearestNeighbors() {
		
	}
	
	public NearestNeighbors(String id, String url) {
		this.id = id;
		this.url = url;
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

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public List<NearestNeighbor> getNearest() {
		return nearest;
	}
	
	public void setNearest(List<NearestNeighbor> nns) {
		this.nearest.addAll(nns);
	}
	
	public static class NearestNeighbor {
		public String id;
		public double distance;
		public double similarity;
		public String url;
		
		public NearestNeighbor() {
			
		}

		public NearestNeighbor(String id, String url, double distance) {
			this.id = id;
			this.url = url;
			this.distance = distance;
			
			this.similarity = 1. - (distance/Math.sqrt(2));
		}
		
	}
	
}