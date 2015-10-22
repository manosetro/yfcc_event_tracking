package gr.iti.mklab.yfcc.geometry.algorithms;

import gr.iti.mklab.yfcc.geometry.utils.IComparator;
import gr.iti.mklab.yfcc.geometry.algorithms.Point2f;

public class XYComparator implements IComparator<Point2f> {

	private double epsilon;

	public XYComparator() {
		this(0.000001);
	}

	public XYComparator(double epsilon) {
		this.epsilon = epsilon;
	}

	/**
       *
	 */
	public int compare(Point2f a, Point2f b){

		assert a != null;
		assert b != null;
		
		int result = compareDouble( a.getY(), b.getY(), this.epsilon );

		if( result == 0 )
			result = compareDouble( a.getX(), b.getX(), this.epsilon );

		return result;
	}

	/*
	 * @.pre {true}
	 * @.post {Jos a < b, RESULT < 0
	 *        jos a = b, RESULT == 0
	 *        jos a > b, RESULT > 0}
	 */	
	private int compareDouble(double a, double b, double eps) {
		double diff = a - b;
		if( -eps < diff && diff < eps )
			return 0;
		if(a < b)
			return -1;
		if(a > b)
			return 1;

		throw new AssertionError("It should be impossible to reach here.");
	}
}