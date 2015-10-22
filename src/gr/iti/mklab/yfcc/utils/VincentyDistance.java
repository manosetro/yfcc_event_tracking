package gr.iti.mklab.yfcc.utils;

import org.apache.commons.math3.ml.distance.DistanceMeasure;

public class VincentyDistance implements DistanceMeasure {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8854367285820467006L;

	@Override
	public double compute(double[] a, double[] b) {
		Double distance = GeodesicDistanceCalculator.vincentyDistance(a[0], a[1], b[0], b[1]);
		if(distance == null) {
			return Double.MAX_VALUE;
		}
		return distance;
	}

}
