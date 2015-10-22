package gr.iti.mklab.yfcc.models;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.ml.clustering.Clusterable;

public class Point implements Clusterable {
	
	private double longitude;
	private double latitude;

	public Point(double longitude, double latitude) {
		this.longitude = longitude;
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	@Override
	public String toString() {
		return "(" + longitude + ", " + latitude + ")";
	}

	public Map<String, Double> toMap() {
		Map<String, Double> map = new HashMap<String, Double>();
		
		map.put("latitude", latitude);
		map.put("longitude", longitude);
		
		return map;
	}

	@Override
	public double[] getPoint() {
		double[] point = new double[2];
		
		point[0] = this.latitude;
		point[1] = this.longitude;
		
		return point;
	}
	
	
}