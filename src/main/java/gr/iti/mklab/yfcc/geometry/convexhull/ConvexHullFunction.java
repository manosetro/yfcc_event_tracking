package gr.iti.mklab.yfcc.geometry.convexhull;

import gr.iti.mklab.yfcc.geometry.algorithms.Point2f;
import gr.iti.mklab.yfcc.geometry.utils.Stack;


public interface ConvexHullFunction {
	public Stack<Point2f> getConvexHull( Point2f[] pts );
}